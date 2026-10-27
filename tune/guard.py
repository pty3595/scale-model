#!/usr/bin/env python3
"""다른 사람의 작업이 클라이언트에서 도는지 본다. busy() 가 클라이언트마다 이 파일을 표준 입력으로 보내 돌린다(원격 쪽은 표준 라이브러리만).

  servers     vllm serve · lmcache server · sglang 프로세스 수
  mooncake    Mooncake 프로세스 수 (mooncake_master · mooncake_store_service · 메타데이터 서버 · transfer_engine_bench ·
              python -m mooncake.*). 우리 trace 이름(mooncake-conversation · traces/mooncake/)은 걸리지 않는다
  ray         raylet 프로세스 수 (Ray 클러스터가 떠 있다)
  gpu_apps    GPU 를 쓰는 프로세스 수 (우리 driver.py 는 뺀다). nvidia-smi 가 없으면(GPU 없는 storage 노드) 0
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def cmdlines():
    out = {}
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            out[int(pid)] = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode(errors="replace").strip()
        except OSError:
            pass
    return out


MOONCAKE = re.compile(r"(^|[\s/])(mooncake[_.]|transfer_engine_bench)")


def look():
    cl = cmdlines()
    me = os.getpid()
    try:
        gpu = subprocess.run(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], capture_output=True, text=True).stdout.split()
    except FileNotFoundError:
        gpu = []
    return {
        "host": os.uname().nodename,
        "servers": sum(1 for p, c in cl.items() if p != me and "bash -c" not in c and any(k in c for k in ("vllm serve", "lmcache server", "sglang"))),
        "mooncake": sum(1 for p, c in cl.items() if p != me and "bash -c" not in c and MOONCAKE.search(c)),
        "ray": sum(1 for c in cl.values() if "/raylet " in c or c.endswith("/raylet")),
        "gpu_apps": sum(1 for p in gpu if p.isdigit() and "driver.py" not in cl.get(int(p), "")),
    }


def busy():
    """클라이언트마다 본다. 다른 작업이 보이면 그 까닭 목록"""
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    import cluster as C
    src, why = Path(__file__).read_text(), []
    for c in C.CLIENTS:
        a = C.addr(c["host"])
        r = subprocess.run(C.remote(a, "python3 -"), input=src, capture_output=True, text=True, check=True)
        g = json.loads(r.stdout)
        if g["servers"] or g["mooncake"] or g["ray"] or g["gpu_apps"]:
            why.append(json.dumps(g))
    return why


if __name__ == "__main__":
    print(json.dumps(look()))
