#!/usr/bin/env python3
"""장비 하나의 하드웨어 카운터를 1초 간격으로 찍어 JSON 한 줄씩 낸다 (표준 라이브러리만 쓴다).

찍는 것: CPU(코어별 jiffies), 프로세스별 CPU(driver.py / hf3fs_fuse_main / storage_main),
NVMe(/proc/diskstats), RDMA 포트 카운터(counters + hw_counters), NIC ethtool -S.
해석(델타 계산)은 hw_probe.py 가 한다.

    python3 hw_sampler.py <초> <RDMA NIC,...>       NIC 가 여럿이면 카운터를 더한다(노드 하나의 합)
"""

import json
import os
import re
import subprocess
import sys
import time

PROCS = ("driver.py", "hf3fs_fuse_main", "storage_main")
# ethtool -S 는 3천 개가 넘는다. 한계를 가리키는 것과 물리 포트 처리량만 남긴다
ETH_KEEP = ("discard", "drop", "out_of_buffer", "pause", "buffer_passed", "cnp", "ecn", "err",
            "bytes_phy", "packets_phy")


def ibdev(nd):
    return os.listdir(f"/sys/class/net/{nd}/device/infiniband")[0]


def cpu():
    out = {}
    for line in open("/proc/stat"):
        if line.startswith("cpu"):
            f = line.split()
            v = list(map(int, f[1:9]))
            out[f[0]] = [sum(v), v[3] + v[4]]          # 전체, idle+iowait
    return out


def procs():
    out = {p: 0 for p in PROCS}
    for pid in filter(str.isdigit, os.listdir("/proc")):
        try:
            cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode()
            for p in PROCS:
                if p in cmd and "hw_sampler" not in cmd and "bash -c" not in cmd:
                    f = open(f"/proc/{pid}/stat").read().rsplit(")", 1)[1].split()
                    out[p] += int(f[11]) + int(f[12])        # utime + stime (jiffies)
        except OSError:
            pass
    return out


def disk():
    """NVMe 장치 전체(파티션 제외)의 합. storage 노드는 NVMe 가 2개다 (2026-10-01 부터)."""
    out = {"sect_r": 0, "sect_w": 0, "io_ticks": 0, "weighted": 0, "n": 0}
    for line in open("/proc/diskstats"):
        f = line.split()
        if re.fullmatch(r"nvme\d+n\d+", f[2]):
            for k, i in (("sect_r", 5), ("sect_w", 9), ("io_ticks", 12), ("weighted", 13)):
                out[k] += int(f[i])
            out["n"] += 1
    return out if out["n"] else None


def rdma(ibs):
    out = {}
    for ib in ibs:
        for sub in ("counters", "hw_counters"):
            d = f"/sys/class/infiniband/{ib}/ports/1/{sub}"
            for name in os.listdir(d):
                try:
                    out[name] = out.get(name, 0) + int(open(f"{d}/{name}").read())
                except (OSError, ValueError):
                    pass
    return out


def ethtool(nds):
    out = {}
    for nd in nds:
        txt = subprocess.run(["/usr/sbin/ethtool", "-S", nd], capture_output=True, text=True).stdout
        for line in txt.splitlines()[1:]:
            k, _, v = line.strip().partition(":")
            k = k.strip()
            if not any(x in k for x in ETH_KEEP):
                continue
            try:
                out[k] = out.get(k, 0) + int(v)
            except ValueError:
                pass
    return out


def main():
    seconds = float(sys.argv[1])
    nd = sys.argv[2].split(",")
    ib = [ibdev(x) for x in nd]
    print(json.dumps({"host": os.uname().nodename, "netdev": ",".join(nd), "ibdev": ",".join(ib),
                      "hz": os.sysconf("SC_CLK_TCK"), "ncpu": os.cpu_count()}), flush=True)
    end = time.time() + seconds
    nxt = time.time()
    while time.time() < end:
        print(json.dumps({"t": time.time(), "cpu": cpu(), "proc": procs(), "disk": disk(),
                          "rdma": rdma(ib), "eth": ethtool(nd)}), flush=True)
        nxt += 1.0
        time.sleep(max(0, nxt - time.time()))


if __name__ == "__main__":
    main()
