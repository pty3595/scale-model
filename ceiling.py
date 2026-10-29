#!/usr/bin/env python3
"""천장 둘 — 하드웨어 구성 + 토폴로지로 계산한 것과, fio-usrbio 로 잰 3FS 경로 천장.

하드웨어 천장: 디스크 · storage 선로 · 클라이언트 선로 중 먼저 차는 곳(payload GiB/s). 재지 않고 계산한다.
  디스크  [hardware] nvme_read_gbps · nvme_write_gbps(NVMe 한 개의 사양값) x target 이 든 디스크. target 이 없는 NVMe 는 세지 않는다
  선로    각 노드 netdevs 의 RDMA port rate(없으면 netdev speed)를 읽는다
  토폴로지 admin_cli list-targets — chunk 가 target 에 고르게 퍼진다고 본다. 읽기는 chain 의 target 하나(read-any),
          쓰기는 chain 의 target 모두(write-all, 복제 수만큼 디스크에 쓰고 storage 가 받는다). target 을 가장 많이 맡은 디스크 · 노드가 먼저 찬다
  PCIe · CPU · 스위치 사이 선로는 세지 않는다

3FS 경로 천장 — 모든 클라이언트가 동시에 fio-usrbio(3FS 네이티브 클라이언트, FUSE 를 거치지 않는다)로 쓰고 읽은 합.
adapter 가 3FS 에서 낼 수 있는 처리량의 위쪽 끝으로, 결과의 "3FS 천장 대비"(tune/analyze.py)의 분모다.
지금 걸린 storage 설정으로 잰다(바꾸지 않는다). 하드웨어 천장보다 많이 낮으면 그 사이가 3FS 소프트웨어 · 설정이다.

  준비  클라이언트마다 job 수만큼 파일(--size)을 fio 가 미리 만든다(FUSE 로 쓴다, 재지 않는다).
        엔진이 O_CREAT 없이 열어 파일이 있어야 하고, 다 써 둔 파일이어야 읽기가 디스크를 거친다(안 쓴 구간은 0 을 돌려준다)
  쓰기  그 파일들을 --seconds 동안 덮어쓴다. 읽기  --seconds 동안 읽는다.
        두 단계 모두 클라이언트가 같은 시각에 출발하고(시계는 replay 처럼 맞아 있다고 본다), 앞 2초는 빼고 잰다
  블록 1 MiB, iodepth 32, O_DIRECT. 끝나면 파일을 지운다

    ceiling.py [--jobs 32] [--size 512M] [--seconds 20]      결과는 clusters/<이름>-ceiling.json

fio-usrbio · 엔진 · libhf3fs_api 는 [builds] fio_usrbio · usrbio_engine · hf3fs_lib
(기본 /opt/3fs/bin/fio-usrbio · /opt/3fs/lib/fio/hf3fs_usrbio.so · /opt/3fs/lib). 없으면 3FS 소스의 benchmarks/fio_usrbio 로 빌드한다.
"""
import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from collections import Counter
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
            f"--mountpoint={shlex.quote(C.MOUNT)} --directory={shlex.quote(d)} --rw={rw} --bs=1M --iodepth=32 --direct=1 "
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


def phase(d, rw, args):
    """모든 클라이언트가 같은 시각에 rw 를 --seconds 동안. 클라이언트별 GiB/s"""
    t0 = time.time() + 5
    wait = f"sleep $(python3 -c 'import time; print(max(0, {t0} - time.time()))') && "
    out = on_all({c["host"]: wait + fio(c["host"], d, rw, args, f"--time_based --runtime={args.seconds} --ramp_time=2")
                  for c in C.CLIENTS})
    key = "read" if rw == "read" else "write"
    return {h: json.loads(o[o.index("{"):])["jobs"][0][key]["bw_bytes"] / GIB for h, o in out.items()}


def link_gbps(host, nds):
    """그 노드 netdevs 의 선로 속도 합(Gb/s) — RDMA port rate, 없으면 netdev speed"""
    cmd = "; ".join(f"ib=$(ls /sys/class/net/{nd}/device/infiniband 2>/dev/null | head -1); "
                    f"p=$(( $(cat /sys/class/net/{nd}/dev_port 2>/dev/null || echo 0) + 1 )); "
                    f"r=$(cut -d' ' -f1 /sys/class/infiniband/$ib/ports/$p/rate 2>/dev/null); "
                    f"echo ${{r:-$(( $(cat /sys/class/net/{nd}/speed 2>/dev/null || echo 0) / 1000 ))}}" for nd in nds)
    v = [float(x) for x in C.sh(C.addr(host), cmd, check=False).split()]
    if len(v) != len(nds) or not all(x > 0 for x in v):
        raise SystemExit(f"{host}: 선로 속도를 못 읽었다 {dict(zip(nds, v))} — [[client]] · [[storage]] netdevs 를 본다")
    return sum(v)


def hardware():
    """하드웨어 구성 + 토폴로지로 계산한 천장. 디스크 사양값이 없으면 디스크는 빼고 선로만으로 계산한다"""
    hw, m = C.C.get("hardware", {}), C.C["measured"]
    rd, wr, rep = hw.get("nvme_read_gbps") or None, hw.get("nvme_write_gbps") or None, int(m["replicas"])
    tg = [l.split() for l in C.admin("list-targets").splitlines()[1:] if l.strip()]   # TargetId ChainId Role ... NodeId DiskIndex UsedSize
    n = len(tg)
    disk = Counter((int(t[5]), t[6]) for t in tg)            # 디스크마다 target 수
    node = Counter(int(t[5]) for t in tg)
    host = {int(s["node_id"]): s["host"] for s in C.STORAGE}
    st = {h: link_gbps(h, s["netdevs"]) for h, s in ((s["host"], s) for s in C.STORAGE)}
    cl = {c["host"]: link_gbps(c["host"], c["netdevs"]) for c in C.CLIENTS}
    gb, gbit = 1e9 / GIB, 1e9 / 8 / GIB
    parts = {
        "read": {"disk": rd * gb * n / max(disk.values()) if rd else None,
                 "storage_link": min(st[host[i]] * gbit * n / k for i, k in node.items()),
                 "client_link": min(cl.values()) * gbit * len(cl)},
        "write": {"disk": wr * gb * n / (rep * max(disk.values())) if wr else None,
                  "storage_link": min(st[host[i]] * gbit * n / (rep * k) for i, k in node.items()),
                  "client_link": min(cl.values()) * gbit * len(cl)},
    }
    nv = C.STORAGE[0]["nvme"][0].rsplit("/", 1)[-1]
    model = C.sh(C.addr(C.STORAGE[0]["host"]), f"cat /sys/block/{nv}/device/model 2>/dev/null", check=False).strip()
    out = {"inputs": {"nvme_model": model, "nvme_read_gbps": rd, "nvme_write_gbps": wr, "replicas": rep, "targets": n,
                      "disks_with_targets": len(disk), "nvme_listed": sum(len(s["nvme"]) for s in C.STORAGE),
                      "storage_link_gbps": st, "client_link_gbps": cl},
           "parts_gibps": parts}
    for rw, p in parts.items():
        known = {k: v for k, v in p.items() if v is not None}
        wall = min(known, key=known.get)
        out[f"{rw}_gibps"], out[f"{rw}_wall"] = known[wall], wall
        print(f"하드웨어 {rw:5} {known[wall]:.2f} GiB/s (먼저 차는 곳 {wall})  " + "  ".join(f"{k} {v:.2f}" if v is not None else f"{k} ?" for k, v in p.items()))
    print(f"  target {n}개(복제 {rep}) · target 이 든 디스크 {len(disk)}개 / 적힌 NVMe {out['inputs']['nvme_listed']}개 · "
          f"선로 storage {' '.join(f'{v:g}' for v in st.values())} · 클라이언트 {' '.join(f'{v:g}' for v in cl.values())} Gb/s")
    if not (rd and wr):
        print(f"!! 디스크 사양값이 없어 선로만으로 계산했다 — NVMe 모델 '{model}' 의 사양서에서 순차 읽기 · 쓰기(GB/s)를 "
              f"clusters/{C.PATH.name} [hardware] nvme_read_gbps · nvme_write_gbps 에 적고 다시 돌린다")
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=32, help="클라이언트마다 job(파일) 수")
    ap.add_argument("--size", default="512M", help="파일 하나의 크기(fio 형식)")
    ap.add_argument("--seconds", type=int, default=20)
    args = ap.parse_args()
    hw = hardware()
    miss = [c["host"] for c in C.CLIENTS
            if C.sh(C.addr(c["host"]), f"test -x {shlex.quote(FIO)} && test -f {shlex.quote(ENGINE)} && echo y", check=False).strip() != "y"]
    if miss:
        raise SystemExit(f"{FIO} 나 {ENGINE} 가 없다: {', '.join(miss)} — 3FS 소스의 benchmarks/fio_usrbio 로 빌드하거나 [builds] 를 고친다")
    why = busy()
    if why:
        raise SystemExit("다른 작업이 보여 멈춘다: " + "; ".join(why))
    size = int(subprocess.run(["numfmt", "--from=iec", args.size.rstrip("Bb")], capture_output=True, text=True, check=True).stdout)
    need = len(C.CLIENTS) * args.jobs * size
    fs = os.statvfs(C.MOUNT)
    if fs.f_bavail * fs.f_frsize - need < 0.15 * fs.f_blocks * fs.f_frsize:
        raise SystemExit(f"3FS 여유가 모자란다 — {need / GIB:.0f} GiB 를 쓰면 15% 아래로 내려간다")
    d = C.data("ceiling")
    try:
        C.sh(None, f"mkdir -p {shlex.quote(d)} && chmod 777 {shlex.quote(d)}")
        print(f"클라이언트 {len(C.CLIENTS)}대 x 파일 {args.jobs}개 x {args.size} 를 만든다 ({need / GIB:.0f} GiB)", flush=True)
        on_all({c["host"]: fio(c["host"], d, "write", args, "--create_only=1 --overwrite=1") for c in C.CLIENTS})   # overwrite 가 없으면 빈 파일만 만든다
        res = {}
        for rw in ("write", "read"):
            res[rw] = phase(d, rw, args)
            print(f"{rw:5} 합 {sum(res[rw].values()):.2f} GiB/s  " + "  ".join(f"{h} {v:.2f}" for h, v in res[rw].items()), flush=True)
        print(f"→ fio ÷ 하드웨어: 읽기 {sum(res['read'].values()) / hw['read_gibps']:.0%} · 쓰기 {sum(res['write'].values()) / hw['write_gibps']:.0%}")
    finally:
        C.sh(None, f"rm -rf {shlex.quote(d)}", check=False)
    OUT.write_text(json.dumps({
        "measured_at": time.strftime("%Y-%m-%d %H:%M"), "tool": "fio-usrbio", "bs": "1M", "iodepth": 32,
        "jobs": args.jobs, "size": args.size, "seconds": args.seconds,
        "read_gibps": sum(res["read"].values()), "write_gibps": sum(res["write"].values()),
        "per_client": {h: {"read": res["read"][h], "write": res["write"][h]} for h in res["read"]},
        "hardware": hw,
    }, ensure_ascii=False, indent=1))
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
