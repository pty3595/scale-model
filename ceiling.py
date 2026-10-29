#!/usr/bin/env python3
"""3FS 경로 천장 — 모든 클라이언트가 동시에 fio-usrbio(3FS 네이티브 클라이언트, FUSE 를 거치지 않는다)로 쓰고 읽은 합.
adapter 가 3FS 에서 낼 수 있는 처리량의 위쪽 끝으로, 결과의 "3FS 천장 대비"(tune/analyze.py)의 분모다.
지금 걸린 storage 설정으로 잰다(바꾸지 않는다).

  크기  데이터 전체가 storage 노드 RAM 합의 두 배 이상이 되게 파일 크기를 정한다(--size 를 주면 그것). 3FS storage 는 O_DIRECT 로
        읽지만 storage 파일시스템이 ZFS 면 캐시를 거친다 — 데이터가 RAM 보다 작으면 천장이 메모리 속도로 나온다(B300, 10-29)
  블록  재생 폴더의 3FS chunk 크기(--bs 를 주면 그것). chunk 보다 작게 쓰면 storage 가 chunk 마다 옛 데이터를 읽어 새 블록에
        통째로 다시 쓴다(copy-on-write, 3FS design_notes "Chunks and the metadata") — 쓰기 천장이 그만큼 낮게 나온다
  준비  클라이언트마다 job 수만큼 파일을 fio 가 미리 만든다(FUSE 로 쓴다, 재지 않는다). 엔진이 O_CREAT 없이 열어 파일이 있어야 한다.
        읽기용(r/)은 다 써 둔다 — 안 쓴 구간은 0 을 돌려줘 디스크를 거치지 않는다. 쓰기용(w/)은 빈 파일이다
  쓰기  빈 파일에 --seconds 동안 이어 쓴다 — LMCache store 처럼 새 파일이라 append(제자리 쓰기)다. 이미 쓴 파일을 덮어쓰면
        copy-on-write 라 더 낮게 나온다. 읽기  다 써 둔 파일을 --seconds 동안 읽는다.
        두 단계 모두 클라이언트가 같은 시각에 출발하고(시계는 replay 처럼 맞아 있다고 본다), 앞 2초는 빼고 잰다.
        같은 구간의 storage NVMe 읽기 · 쓰기도 잰다 — 읽기에서 디스크가 fio 보다 10% 넘게 적으면 캐시에서 온 것이다
  iodepth 32, O_DIRECT. 끝나면 파일을 지운다
  쓰기는 chain 의 모든 복제에 차례로 가고 읽기는 아무 복제에서나 한다(CRAQ) — 쓰기 천장은 대개 읽기의 1/복제 수 근처다

    ceiling.py [--jobs 32] [--size 4G] [--bs 1M] [--seconds 20]      결과는 clusters/<이름>-ceiling.json

fio-usrbio · 엔진 · libhf3fs_api 는 [builds] fio_usrbio · usrbio_engine · hf3fs_lib
(기본 /opt/3fs/bin/fio-usrbio · /opt/3fs/lib/fio/hf3fs_usrbio.so · /opt/3fs/lib). 없으면 3FS 소스의 benchmarks/fio_usrbio 로 빌드한다.
"""
import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "tune"))
import cluster as C                   # noqa: E402
from guard import busy                # noqa: E402

GIB = 2**30
B = C.C.get("builds", {})
FIO = B.get("fio_usrbio", "/opt/3fs/bin/fio-usrbio")
ENGINE = B.get("usrbio_engine", "/opt/3fs/lib/fio/hf3fs_usrbio.so")
LIB = B.get("hf3fs_lib", "/opt/3fs/lib")
OUT = C.DIR / f"{C.PATH.stem}-ceiling.json"


def fio(host, d, rw, args, extra=""):
    return (f"LD_LIBRARY_PATH={shlex.quote(LIB)} {shlex.quote(FIO)} --name={host} --ioengine=external:{shlex.quote(ENGINE)} "
            f"--mountpoint={shlex.quote(C.MOUNT)} --directory={shlex.quote(d)} --rw={rw} --bs={args.bs} --iodepth=32 --direct=1 "
            f"--numjobs={args.jobs} --size={args.size} --group_reporting --output-format=json {extra}")


def on_all(cmds):
    """클라이언트마다 명령을 함께 띄워 stdout 을 모은다"""
    ps = {h: C.run_on(C.addr(h), c, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for h, c in cmds.items()}
    out = {}
    for h, p in ps.items():
        o, e = p.communicate()
        if p.returncode:
            raise SystemExit(f"{h}: fio 실패 ({p.returncode}) {e.strip()[-300:]}")
        out[h] = o
    return out


def disk_sampler(t_from, seconds):
    """storage 노드마다 t_from 부터 seconds 동안의 NVMe 읽기 · 쓰기를 재는 프로세스(끝나면 disk_rates 로 읽는다)"""
    cmd = (f"sleep $(python3 -c 'import time; print(max(0, {t_from} - time.time()))') && cat /proc/diskstats && echo === "
           f"&& sleep {seconds} && cat /proc/diskstats")
    return {s["host"]: (s, C.run_on(C.addr(s["host"]), cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True))
            for s in C.STORAGE}


def disk_rates(procs, seconds):
    """disk_sampler 의 결과 → 노드마다 (읽기, 쓰기) GiB/s"""
    out = {}
    for h, (s, p) in procs.items():
        o, _ = p.communicate()
        devs = {Path(x).name for x in s["nvme"]}
        a, b = ({f[2]: (int(f[5]), int(f[9])) for f in (l.split() for l in part.splitlines()) if len(f) > 9 and f[2] in devs}
                for part in o.split("==="))
        out[h] = tuple(sum(b[d][i] - a.get(d, b[d])[i] for d in b) * 512 / GIB / seconds for i in (0, 1))
    return out


def layout(d):
    """재생 폴더의 3FS layout (chunk 바이트, stripe) 과 chain 의 복제 수"""
    lay = {l.split()[0]: l.split()[-1] for l in C.admin(f"stat --display-layout {C.rel(d)}").splitlines() if l.startswith("Layout-")}
    chains = [l for l in C.admin("list-chains").splitlines()[1:] if l.strip()]
    reps = len(re.findall(r"\d+\([A-Z-]+\)", chains[0])) if chains else None
    return int(lay["Layout-ChunkSize"]), int(lay["Layout-StripeSize"]), reps


def phase(d, rw, args):
    """모든 클라이언트가 같은 시각에 rw 를 --seconds 동안. 클라이언트별 GiB/s, storage 노드별 (읽기, 쓰기) GiB/s"""
    t0 = time.time() + 5
    disk = disk_sampler(t0 + 2, args.seconds)                 # fio 의 ramp 2초 뒤부터 같은 구간
    wait = f"sleep $(python3 -c 'import time; print(max(0, {t0} - time.time()))') && "
    out = on_all({c["host"]: wait + fio(c["host"], d, rw, args, f"--time_based --runtime={args.seconds} --ramp_time=2")
                  for c in C.CLIENTS})
    key = "read" if rw == "read" else "write"
    return ({h: json.loads(o[o.index("{"):])["jobs"][0][key]["bw_bytes"] / GIB for h, o in out.items()},
            disk_rates(disk, args.seconds))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=32, help="클라이언트마다 job(파일) 수")
    ap.add_argument("--size", help="파일 하나의 크기(fio 형식). 없으면 데이터 전체가 storage RAM 합의 두 배가 되게 정한다")
    ap.add_argument("--bs", help="fio 블록(fio 형식). 없으면 재생 폴더의 3FS chunk 크기")
    ap.add_argument("--seconds", type=int, default=20)
    args = ap.parse_args()
    miss = [c["host"] for c in C.CLIENTS
            if C.sh(C.addr(c["host"]), f"test -x {shlex.quote(FIO)} && test -f {shlex.quote(ENGINE)} && echo y", check=False).strip() != "y"]
    if miss:
        raise SystemExit(f"{FIO} 나 {ENGINE} 가 없다: {', '.join(miss)} — 3FS 소스의 benchmarks/fio_usrbio 로 빌드하거나 [builds] 를 고친다")
    why = busy()
    if why:
        raise SystemExit("다른 작업이 보여 멈춘다: " + "; ".join(why))
    ram = sum(int(C.sh(C.addr(s["host"]), "awk '/^MemTotal:/{print $2}' /proc/meminfo")) * 1024 for s in C.STORAGE)
    if args.size:
        size = int(subprocess.run(["numfmt", "--from=iec", args.size.rstrip("Bb")], capture_output=True, text=True, check=True).stdout)
    else:
        size = -(-2 * ram // (len(C.CLIENTS) * args.jobs * 2**20)) * 2**20      # MiB 단위로 올림
        args.size = f"{size >> 20}M"
    need = len(C.CLIENTS) * args.jobs * size
    print(f"storage RAM 합 {ram / GIB:.0f} GiB, 데이터 {need / GIB:.0f} GiB({need / ram:.1f}배)"
          + ("" if need >= 2 * ram else " — RAM 의 두 배보다 작아 캐시에서 읽을 수 있다"), flush=True)
    fs = os.statvfs(C.MOUNT)
    if fs.f_bavail * fs.f_frsize - 2 * need < 0.15 * fs.f_blocks * fs.f_frsize:      # 읽기용 + 쓰기용(다 차는 최악)
        raise SystemExit(f"3FS 여유가 모자란다 — {2 * need / GIB:.0f} GiB 를 쓰면 15% 아래로 내려간다")
    d = C.data("ceiling")
    try:
        C.sh(None, f"mkdir -p {shlex.quote(d)}/r {shlex.quote(d)}/w && chmod -R 777 {shlex.quote(d)}")
        chunk, stripe, reps = layout(d)
        args.bs = args.bs or f"{chunk >> 10}K"
        print(f"3FS layout chunk {chunk >> 10} KiB · stripe {stripe} · 복제 {reps} → fio 블록 {args.bs}", flush=True)
        print(f"클라이언트 {len(C.CLIENTS)}대 x 파일 {args.jobs}개 x {args.size} 를 만든다 ({need / GIB:.0f} GiB, 읽기용)", flush=True)
        on_all({c["host"]: fio(c["host"], f"{d}/r", "write", args, "--create_only=1 --overwrite=1") for c in C.CLIENTS})
        on_all({c["host"]: fio(c["host"], f"{d}/w", "write", args, "--create_only=1") for c in C.CLIENTS})   # overwrite 가 없으면 빈 파일
        res, disk = {}, {}
        for rw in ("write", "read"):
            res[rw], disk[rw] = phase(f"{d}/{rw[0]}", rw, args)
            dr, dw = (sum(x[i] for x in disk[rw].values()) for i in (0, 1))
            print(f"{rw:5} 합 {sum(res[rw].values()):.2f} GiB/s  " + "  ".join(f"{h} {v:.2f}" for h, v in res[rw].items())
                  + f"  | storage 디스크 읽기 {dr:.2f} · 쓰기 {dw:.2f} GiB/s", flush=True)
        print(f"쓰기 ÷ 읽기 {sum(res['write'].values()) / sum(res['read'].values()):.2f} (복제 {reps} — 쓰기는 모든 복제에, 읽기는 아무 복제에서)",
              flush=True)
        cached = sum(x[0] for x in disk["read"].values()) < 0.9 * sum(res["read"].values())
        if cached:
            print("!! 읽기에서 storage 디스크가 fio 보다 10% 넘게 적다 — 일부가 storage 메모리(캐시)에서 왔다. --size 를 키우거나 "
                  "storage 파일시스템(ZFS 면 캐시를 거친다)을 본다", flush=True)
    finally:
        C.sh(None, f"rm -rf {shlex.quote(d)}", check=False)
    OUT.write_text(json.dumps({
        "measured_at": time.strftime("%Y-%m-%d %H:%M"), "tool": "fio-usrbio", "bs": args.bs, "iodepth": 32,
        "chunk_bytes": chunk, "stripe": stripe, "replicas": reps, "write_mode": "append(새 파일)",
        "jobs": args.jobs, "size": args.size, "seconds": args.seconds,
        "read_gibps": sum(res["read"].values()), "write_gibps": sum(res["write"].values()),
        "per_client": {h: {"read": res["read"][h], "write": res["write"][h]} for h in res["read"]},
        "data_gib": need / GIB, "storage_ram_gib": ram / GIB,
        "storage_disk_gibps": {rw: {h: {"read": v[0], "write": v[1]} for h, v in disk[rw].items()} for rw in disk},
        "read_from_cache": cached,
    }, ensure_ascii=False, indent=1))
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
