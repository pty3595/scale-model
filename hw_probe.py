#!/usr/bin/env python3
"""클라이언트 여러 대에서 adapter 를 제한 없이 동시에 쏘면서 장비별 하드웨어 한계를 본다.

클라이언트와 storage 노드 전부에 hw_sampler.py 를 띄우고, 클라이언트마다 driver 를
제한 없이(--plan 없이) 같은 시각에 출발시킨다. 끝나면 모두가 돌던 구간에서
CPU·디스크·RoCE 카운터를 장비별로 요약한다.

    python3 hw_probe.py --op load --rounds 30 --out results/hw-limits/load
"""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

import cluster as C

GIB = 1024**3
HERE = Path(__file__).resolve().parent
STORAGE = {s["host"]: C.addr(s["host"]) for s in C.STORAGE}     # 이름 → 주소(이 장비면 None)
SSH = C.SSH                     # 뒤에 user@주소 를 붙인다(C.ssh)

# 한계를 가리키는 카운터. 부하 구간에서 늘었으면 보고한다.
RDMA_ERR = ("out_of_sequence", "packet_seq_err", "implied_nak_seq_err", "roce_adp_retrans",
            "roce_adp_retrans_to", "local_ack_timeout_err", "duplicate_request", "np_cnp_sent",
            "rp_cnp_handled", "np_ecn_marked_roce_packets", "out_of_buffer", "rnr_nak_retry_err",
            "req_cqe_error", "resp_cqe_error", "roce_slow_restart")
ETH_KEYS = ("discard", "drop", "out_of_buffer", "pause", "buffer_passed", "cnp", "ecn", "err")


run_on = C.run_on


def sampler_cmd(host: str, seconds: float) -> str:
    n = C.NODES[host]
    disks = ",".join(os.path.basename(d) for d in n.get("nvme", [])) or "-"      # storage 의 3FS NVMe 만 잰다
    return (f"nohup python3 /tmp/hw_sampler.py {seconds} {','.join(n['netdevs'])} {disks} > /tmp/hw_sampler.out 2>&1 & "
            "echo $! > /tmp/hw_sampler.pid")


def summarize(rows: list, meta: dict, a: float, b: float) -> dict:
    w = [r for r in rows if a <= r["t"] <= b]
    if len(w) < 3:
        return {"error": "구간 샘플이 모자라다"}
    first, last = w[0], w[-1]
    dt = last["t"] - first["t"]
    hz = meta["hz"]

    def busy(r0, r1, key):
        tot = r1["cpu"][key][0] - r0["cpu"][key][0]
        idle = r1["cpu"][key][1] - r0["cpu"][key][1]
        return 100 * (1 - idle / tot) if tot else 0.0

    # 초 단위로 가장 바쁜 코어를 찾아 평균·최대
    core_max = []
    for r0, r1 in zip(w, w[1:]):
        cores = [k for k in r1["cpu"] if k != "cpu"]
        core_max.append(max(busy(r0, r1, k) for k in cores))
    out = {
        "window_s": dt,
        "cpu_total_pct": busy(first, last, "cpu"),
        "cpu_busiest_core_pct_mean": sum(core_max) / len(core_max),
        "cpu_busiest_core_pct_max": max(core_max),
        "proc_cores": {p: (last["proc"][p] - first["proc"][p]) / hz / dt
                       for p in last["proc"] if last["proc"][p] - first["proc"][p] > 0},
    }
    if last.get("disk"):
        d0, d1 = first["disk"], last["disk"]
        out["disk_read_gibps"] = (d1["sect_r"] - d0["sect_r"]) * 512 / GIB / dt
        out["disk_write_gibps"] = (d1["sect_w"] - d0["sect_w"]) * 512 / GIB / dt
        out["disk_util_pct"] = 100 * (d1["io_ticks"] - d0["io_ticks"]) / 1000 / dt / d1.get("n", 1)   # 장치 평균
        out["disk_avg_queue"] = (d1["weighted"] - d0["weighted"]) / 1000 / dt
    r0, r1 = first["rdma"], last["rdma"]
    out["roce_rx_gibps"] = (r1["port_rcv_data"] - r0["port_rcv_data"]) * 4 / GIB / dt
    out["roce_tx_gibps"] = (r1["port_xmit_data"] - r0["port_xmit_data"]) * 4 / GIB / dt
    out["rdma_events"] = {k: r1[k] - r0[k] for k in RDMA_ERR if k in r1 and r1[k] - r0[k]}
    e0, e1 = first["eth"], last["eth"]
    out["eth_events"] = {k: e1[k] - e0[k] for k in e1
                         if any(s in k for s in ETH_KEYS) and k in e0 and e1[k] - e0[k]}
    return out


def report(summary: dict) -> None:
    print(f"{summary["op"]}  구간 {summary["window"][1] - summary["window"][0]:.0f}초  클라이언트별 "
          + "  ".join(f"{h} {g:.3f}" for h, g in summary["per_client_gibps"].items())
          + f"  합계 {sum(summary['per_client_gibps'].values()):.3f} GiB/s")
    for h, s in summary["hosts"].items():
        if "error" in s:
            print(f"  {h:9} {s['error']}")
            continue
        disk = (f"  disk r {s['disk_read_gibps']:.2f} w {s['disk_write_gibps']:.2f} GiB/s "
                f"util {s['disk_util_pct']:.0f}% q {s['disk_avg_queue']:.1f}" if "disk_util_pct" in s else "")
        print(f"  {h:9} cpu {s['cpu_total_pct']:4.1f}% (최대 코어 평균 {s['cpu_busiest_core_pct_mean']:.0f}% "
              f"/ 최고 {s['cpu_busiest_core_pct_max']:.0f}%)  "
              + " ".join(f"{k}={v:.2f}코어" for k, v in s["proc_cores"].items())
              + f"  RoCE rx {s['roce_rx_gibps']:.2f} tx {s['roce_tx_gibps']:.2f} GiB/s" + disk)
        if s["rdma_events"]:
            print(f"            RDMA {s['rdma_events']}")
        if s["eth_events"]:
            print(f"            NIC  {s['eth_events']}")



def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--op", choices=["load", "store"], default="load")
    ap.add_argument("--rounds", type=int, default=30)
    ap.add_argument("--clients", default=",".join(c["host"] for c in C.CLIENTS))
    ap.add_argument("--num-keys", type=int, default=None, help="묶음 크기 (기본: 트레이스 값)")
    ap.add_argument("--backend-param", action="append", default=[], metavar="K=V",
                    help="adapter backend_params 덮어쓰기, driver 에 그대로 넘긴다")
    ap.add_argument("--stripe", type=int, default=None,
                    help="데이터 폴더의 stripe. 주면 클라이언트마다 <host>-s<N> 폴더에 layout 을 걸고 load 데이터셋을 먼저 만든다")
    ap.add_argument("--chunk", type=int, default=None,
                    help="데이터 폴더의 chunk 크기(바이트). --stripe 와 함께 주면 <host>-s<N>-c<KiB>k 폴더를 쓴다")
    ap.add_argument("--prepare", action="store_true",
                    help="--stripe 폴더가 이미 있어도 load 데이터셋을 다시 쓴다 (기본: 폴더를 새로 만들 때만)")
    ap.add_argument("--prefix", default="",
                    help="driver 명령 앞에 붙일 것. 예: 'LD_PRELOAD=/tmp/pwrite_full.so taskset -c 0-9'")
    ap.add_argument("--out", required=True)
    ap.add_argument("--report", action="store_true", help="--out 의 summary.json 을 다시 출력만 한다")
    args = ap.parse_args()
    if args.report:
        report(json.loads((Path(args.out) / "summary.json").read_text()))
        return

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    by_host = {c["host"]: c for c in C.CLIENTS}
    clients = [by_host[h] for h in args.clients.split(",")]
    cal_text = json.dumps(C.calibration(), ensure_ascii=False)

    hosts = {c["host"]: C.addr(c["host"]) for c in clients}
    hosts.update({h: a for h, a in STORAGE.items() if h not in hosts})
    sampler = (HERE / "hw_sampler.py").read_text()
    for h, ip in hosts.items():
        if ip is not None:
            subprocess.run(C.remote(ip, "cat > /tmp/hw_sampler.py"), input=sampler, text=True, check=True)
            if h in by_host:
                subprocess.run(C.remote(ip, "cat > /tmp/scale-model-calibration.json"), input=cal_text, text=True, check=True)
        else:
            Path("/tmp/hw_sampler.py").write_text(sampler)
            Path("/tmp/scale-model-calibration.json").write_text(cal_text)

    # 데이터 폴더는 클라이언트마다. store 는 매번 새 key 를 쓰고 끝나면 지운다
    data_dir = {c["host"]: f" --data-dir {C.data(c['host'])}" + ("-store" if args.op == "store" else "") for c in clients}
    if args.stripe:
        from replay import ensure_layout
        created = False
        for c in clients:
            d = (C.data(f"{c['host']}-s{args.stripe}") + (f"-c{args.chunk >> 10}k" if args.chunk else "")
                 + ("-store" if args.op == "store" else ""))
            created |= ensure_layout(d, args.stripe, args.chunk)
            data_dir[c["host"]] = f" --data-dir {d}"
        if args.op == "load" and (created or args.prepare):
            preps = [run_on(hosts[c["host"]], f"cd {c['workdir']} && {c['python']} driver.py --mode prepare --calibration /tmp/scale-model-calibration.json "
                     f"--dataset-rounds 8{data_dir[c['host']]} --quiet --out /tmp/scale-model-hwprobe-prep.json "
                     "> /dev/null 2>&1") for c in clients]
            for p in preps:
                p.wait()

    est = args.rounds * (2.3 if args.op == "load" else 0.8)
    # 샘플러 출력은 초당 10 KB 넘게 나온다. 파이프로 받으면 버퍼가 차서 샘플러가 멈추므로
    # 각 장비의 파일에 쓰고 끝난 뒤 가져온다.
    for h, ip in hosts.items():
        run_on(ip, sampler_cmd(h, est + 120)).wait()
    start_at = time.time() + 35       # 샘플러 기준선 + adapter 생성 + index 패스
    drivers = {}
    for c in clients:
        cmd = (f"cd {c['workdir']} && {args.prefix} {c['python']} driver.py --mode run --op {args.op} --calibration /tmp/scale-model-calibration.json "
               f"--in-flight 1 --dataset-rounds 8 --rounds {args.rounds} --start-at {start_at} "
               f"--quiet --out /tmp/scale-model-hwprobe-{c['host']}.json"
               + (f" --num-keys {args.num_keys}" if args.num_keys else "") + data_dir[c["host"]]
               + "".join(f" --backend-param {kv}" for kv in args.backend_param))
        drivers[c["host"]] = run_on(hosts[c["host"]], cmd)
    for h, p in drivers.items():
        if p.wait() != 0:
            raise SystemExit(f"{h} driver 가 실패했다")
    time.sleep(3)
    raw, meta = {}, {}
    for h, ip in hosts.items():
        run_on(ip, "kill $(cat /tmp/hw_sampler.pid) 2>/dev/null; true").wait()   # pkill -f 는 자기 셸까지 맞힌다
        text = run_on(ip, "cat /tmp/hw_sampler.out", stdout=subprocess.PIPE,
                      text=True).communicate()[0]
        lines = []
        for l in text.splitlines():
            try:
                lines.append(json.loads(l))
            except json.JSONDecodeError:
                pass                     # 샘플러를 끌 때 잘린 마지막 줄
        meta[h], raw[h] = lines[0], lines[1:]
        (out / f"raw-{h}.jsonl").write_text("\n".join(json.dumps(x) for x in lines))

    drv = {}
    for c in clients:
        path = f"/tmp/scale-model-hwprobe-{c['host']}.json"
        txt = (Path(path).read_text() if hosts[c["host"]] is None else
               subprocess.run(C.remote(c["addr"], f"cat {path}"),
                              capture_output=True, text=True, check=True).stdout)
        drv[c["host"]] = json.loads(txt)
        (out / f"driver-{c['host']}.json").write_text(txt)
    if args.op == "store":
        for c in clients:
            d = drv[c["host"]]["dataset"]["dir"]
            assert d.endswith("-store"), d
            run_on(hosts[c["host"]], f"find {d} -mindepth 1 -delete").wait()

    # 모두가 돌던 구간: 출발 후 5초 ~ 가장 먼저 끝난 클라이언트의 끝 2초 전
    end_first = min(start_at + sum(drv[h][args.op]["round_durations_s"]) for h in drv)
    a, b = start_at + 5, end_first - 2
    summary = {"op": args.op, "clients": args.clients, "window": [a, b],
               "num_keys": args.num_keys, "backend_param": args.backend_param,
               "per_client_gibps": {h: drv[h][args.op]["mean_busy_gibps"] for h in drv},
               "hosts": {h: summarize(raw[h], meta[h], a, b) for h in hosts}}
    (out / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    report(summary)

if __name__ == "__main__":
    main()
