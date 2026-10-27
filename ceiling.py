#!/usr/bin/env python3
"""3FS 경로 천장 — 모든 클라이언트가 동시에 fio-usrbio(3FS 네이티브 클라이언트, FUSE 를 거치지 않는다)로 쓰고 읽은 합.
adapter 가 3FS 에서 낼 수 있는 처리량의 위쪽 끝으로, 결과의 "3FS 천장 대비"(tune/analyze.py)의 분모다.
지금 걸린 storage 설정으로 잰다(바꾸지 않는다).

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


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--jobs", type=int, default=32, help="클라이언트마다 job(파일) 수")
    ap.add_argument("--size", default="512M", help="파일 하나의 크기(fio 형식)")
    ap.add_argument("--seconds", type=int, default=20)
    args = ap.parse_args()
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
    finally:
        C.sh(None, f"rm -rf {shlex.quote(d)}", check=False)
    OUT.write_text(json.dumps({
        "measured_at": time.strftime("%Y-%m-%d %H:%M"), "tool": "fio-usrbio", "bs": "1M", "iodepth": 32,
        "jobs": args.jobs, "size": args.size, "seconds": args.seconds,
        "read_gibps": sum(res["read"].values()), "write_gibps": sum(res["write"].values()),
        "per_client": {h: {"read": res["read"][h], "write": res["write"][h]} for h in res["read"]},
    }, ensure_ascii=False, indent=1))
    print(f"→ {OUT}")


if __name__ == "__main__":
    main()
