#!/usr/bin/env python3
"""plan.py 가 낸 plan 을 클라이언트 여러 대에서 같은 시각에 replay 하고 결과를 잰다.

  - plan 을 각 클라이언트에 보내고, prepopulate chunk 를 쓴 뒤 같은 시각에 출발시킨다
  - 클라이언트와 storage 전부에 hw_sampler.py 를 띄운다 (CPU·디스크·RDMA·NIC)
  - 끝나면 plan 한 load/store 양과 storage 디스크가 실제로 읽고 쓴 양, request 시작 지연,
    request 당 load 시간(lookup 포함), store 한 묶음의 지연, 사용률(디스크 읽기·쓰기, 클라이언트·storage 링크)을
    summary.json 에 쓴다.
    링크 사용률은 payload 기준과 선로 기준(RDMA 포트 카운터, 헤더·재전송 포함) 둘 다 낸다
  - replay 데이터(공유 폴더)는 끝나면 비운다
  plan 폴더를 주면 sweep 단계를 차례로 돌린다 (결과는 --out 아래 단계별 폴더)

    python3 replay.py plans/coder --out results/coder
    python3 replay.py plans/coder/read0.10.json --out results/coder-read0.10
"""

import argparse
import json
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import cluster as C
from hw_probe import STORAGE, run_on, sampler_cmd, summarize
from plan import util_ceilings, util_of

GIB = 1024**3
HERE = Path(__file__).resolve().parent
DATA_DIR = C.data("replay")
REMOTE_CAL = "/tmp/scale-model-calibration.json"    # driver 가 읽는 calibration(이 클러스터의 값을 얹은 것). 모든 클라이언트에 같은 글
# 대당 미리 쓰기 GiB/s — 출발 시각을 정하는 어림. 기본은 PoC(spark)에서 잰 값이고 clusters/<이름>.toml 의 [prepop_gibps] 로 바꾼다.
# 느리게 잡으면 출발만 늦고, 빠르게 잡으면 미리 쓰기가 덜 끝나 그 실행이 무효다(start_gap 음수)
# dram 0.35 · dram_zc 1.2(2026-10-08, 89 GiB 55초에 약 1.65) · hf3fs 1.5(2026-10-12, 한 대만 쓸 때 약 3.2)
PREPOP_GIBPS = dict(dict(dram=0.35, dram_zc=1.2, hf3fs=1.5), **C.C.get("prepop_gibps", {}))


def files_under(d: str) -> set:
    """재생 폴더 아래 파일의 상대 경로. adapter 가 폴더를 여럿 쓰면(base_paths) 하위 폴더까지 센다."""
    return {os.path.relpath(os.path.join(r, f), d) for r, _, fs in os.walk(d) for f in fs}


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


admin = C.admin


def ensure_layout(path: str, stripe: int, chunk: int | None = None) -> bool:
    """폴더를 만들고 stripe(주면 chunk 크기도)를 지정한다. 이미 있으면 맞는지만 본다. 새로 만들었으면 True."""
    rel = C.rel(path)
    created = not Path(path).exists()
    if created:
        Path(path).mkdir(parents=True)
        admin(f"set-layout {rel} --stripe-size {stripe}" + (f" --chunk-size {chunk}" if chunk else ""))
        Path(path).chmod(0o777)       # 다른 클라이언트의 spark 계정도 쓴다
    lay = {l.split()[0]: l.split()[-1] for l in admin(f"stat --display-layout {rel}").splitlines()
           if l.startswith("Layout-")}
    if lay.get("Layout-StripeSize") != str(stripe) or (chunk and lay.get("Layout-ChunkSize") != str(chunk)):
        raise SystemExit(f"{path} 의 layout 이 stripe {lay.get('Layout-StripeSize')} · chunk {lay.get('Layout-ChunkSize')} 다 "
                         f"(stripe {stripe}" + (f" · chunk {chunk}" if chunk else "") + " 이어야 한다)")
    return created


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("plan")
    ap.add_argument("--hosts", default=None, help="쉼표로 구분 (기본: calibration 의 런타임 클라이언트, 이 장비 먼저)")
    ap.add_argument("--l1-align", type=int, default=None, help="driver 의 L1 정렬 바이트 (기본: calibration 8 MiB, LMCache MP 기본 4096)")
    ap.add_argument("--slots", type=int, default=8, help="lookup·load 를 동시에 진행하는 요청 수 (LMCache prefetch 한도)")
    ap.add_argument("--procs", type=int, default=1,
                    help="클라이언트마다 driver 프로세스 수. event 를 대화 단위로 나눠 맡는다 (--slots 는 프로세스마다). "
                         "adapter 의 프로세스 단위 한계(store)를 걷어 최대 배속을 올린다")
    ap.add_argument("--stripe", type=int, default=None,
                    help="재생 폴더의 stripe (기본: 3FS 기본값, calibration.json layout.default_stripe). A/B 에서 스토리지 쪽 설정으로 쓴다")
    ap.add_argument("--chunk", type=int, default=None, help="재생 폴더의 chunk 크기(바이트). --stripe 와 함께 준다")
    ap.add_argument("--max-batch", type=int, default=64, help="load 한 번에 보낼 최대 chunk 수, 0 이면 request 당 task 하나 — LMCache prefetch 와 같다 (driver 로 넘긴다)")
    ap.add_argument("--disk-f", type=float, default=1.0,
                    help="이 실행에 건 디스크 상한 배율(throttle.sh stor). 사용률 분모를 그만큼 줄인다")
    ap.add_argument("--net-f", type=float, default=1.0,
                    help="이 실행에 건 망 상한 배율(throttle.sh net GBIT 이면 GBIT/100). 사용률 분모를 그만큼 줄인다")
    ap.add_argument("--load-rdonly", action="store_true",
                    help="load 할 파일을 읽기 전용으로 연다 (driver 로 넘긴다). adapter 는 O_RDWR 로 연다")
    ap.add_argument("--backend-param", action="append", default=[], metavar="K=V",
                    help="adapter backend_params 덮어쓰기 (driver 로 넘긴다). 예: mem_config=dram_zc")
    ap.add_argument("--prefix", default="",
                    help="driver 명령 앞에 붙일 것. 예: 'LD_PRELOAD=/tmp/pwrite_full.so'")
    ap.add_argument("--adapter-json", default=None,
                    help="calibration 의 adapter 대신 쓸 adapter JSON (driver 로 넘긴다). {dir} 는 재생 폴더로 바뀐다")
    ap.add_argument("--python", default=None,
                    help="driver 를 띄울 python (기본: calibration 의 클라이언트별 값). 클라이언트의 셸이 푼다. 예: '$HOME/venv-lmcache-hf3fs/bin/python'")
    ap.add_argument("--data-dir", default=None,
                    help="재생 폴더 (기본: " + DATA_DIR + "). plan 묶음마다 폴더를 따로 두고 --keep-prepop 과 함께 쓴다")
    ap.add_argument("--keep-prepop", action="store_true",
                    help="미리 쓴 chunk 를 지우지 않고 다음 실행이 다시 쓴다. 끝나면 이 실행이 새로 쓴 파일만 지운다. "
                         "같은 plan 묶음(trace 구간 · 서빙 설정이 같은 것)을 단계만 바꿔 여러 번 돌릴 때 쓴다")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    src = Path(args.plan)
    if src.is_dir():              # sweep: 단계를 차례로
        index = json.loads((src / "index.json").read_text())
        for lv in index["levels"]:
            if "file" in lv:
                print(f"== {lv['level']}", flush=True)
                replay_one(src / lv["file"], Path(args.out) / Path(lv["file"]).stem, args)
    else:
        replay_one(src, Path(args.out), args)


def replay_one(plan_path: Path, out: Path, args) -> None:
    out.mkdir(parents=True, exist_ok=True)
    plan = json.loads(plan_path.read_text())
    cal = C.calibration()
    by_host = {c["host"]: c for c in C.CLIENTS}
    if args.hosts:
        hosts = args.hosts.split(",")
    else:
        hosts = ([C.ME] if C.ME in by_host else []) + [h for h in by_host if h != C.ME]
    hosts = hosts[:plan["clients"]]
    if len(hosts) != plan["clients"]:
        raise SystemExit(f"계획은 {plan['clients']}대인데 클라이언트가 {len(hosts)}대다")
    clients = [by_host[h] for h in hosts]
    ip = {c["host"]: C.addr(c["host"]) for c in clients}
    ip.update({h: a for h, a in STORAGE.items() if h not in ip})     # storage 가 클라이언트도 맡으면 장비 하나

    data_dir = args.data_dir or DATA_DIR
    if args.stripe:
        data_dir = f"{DATA_DIR}-s{args.stripe}" + (f"-c{args.chunk >> 10}k" if args.chunk else "")
        ensure_layout(data_dir, args.stripe, args.chunk)

    # 계획과 샘플러를 보낸다
    sampler = (HERE / "hw_sampler.py").read_text()
    plan_text = plan_path.read_text()
    # 원격 클라이언트의 driver 와 calibration 은 매번 이 사본으로 덮는다 — 따로 두면 낡는다 (10-21: 멈춤 한도가 spark4 에만 있었다)
    driver_text, cal_text = (HERE / "driver.py").read_text(), json.dumps(cal, ensure_ascii=False)
    # 죽은 프로세스가 남긴 hf3fs iov(/dev/shm)를 지운다 — 10-22: 10-21 새벽 멈춘 replay 가 남긴 30 GB 로 nixl_native dram_zc 가 못 떴다.
    # 이 계정 소유 · 5분 넘은 것 · 살아 있는 프로세스가 매핑하지 않은 것만 지운다(validate/clean_iov.sh)
    clean = (HERE / "validate/clean_iov.sh").read_text()
    remote_plan = "/tmp/scale-model-replay-plan.json"
    for h, addr in ip.items():
        if h in hosts:
            subprocess.run(C.remote(addr, f"SM_MOUNT={C.MOUNT} bash -s"), input=clean, text=True, check=True, stdout=subprocess.DEVNULL)
        if addr is None:
            Path("/tmp/hw_sampler.py").write_text(sampler)
            Path(remote_plan).write_text(plan_text)
            Path(REMOTE_CAL).write_text(cal_text)
        else:
            subprocess.run(C.remote(addr, "cat > /tmp/hw_sampler.py"), input=sampler, text=True, check=True)
            if h in hosts:
                for f, text in ((remote_plan, plan_text), (REMOTE_CAL, cal_text), (f"{by_host[h]['workdir']}/driver.py", driver_text)):
                    subprocess.run(C.remote(addr, f"cat > {f}"), input=text, text=True, check=True)

    pids0 = C.storage_pids()       # 끝난 뒤와 다르면 그 사이 storage 가 다시 떴다(hot-update 가 풀린다) — 이 실행은 무효
    # 미리 쓰기가 끝날 시간을 어림해 출발 시각을 정한다
    prep_gib = max(len(p) for p in plan["prepopulate"]) * plan["chunk_bytes"] / GIB
    if args.keep_prepop and os.path.isdir(data_dir) and len(files_under(data_dir)) >= sum(len(p) for p in plan["prepopulate"]):
        prep_gib = 0          # 앞 실행이 남긴 것을 그대로 쓴다. 빠진 것이 있으면 driver 가 쓰고, 늦으면 start_gap_s 가 음수로 남는다
    wall = PREPOP_GIBPS["hf3fs" if '"hf3fs"' in (args.adapter_json or "") else
                        "dram_zc" if "mem_config=dram_zc" in args.backend_param else "dram"]
    start_at = time.time() + 40 + 1.5 * prep_gib / wall
    total = start_at - time.time() + plan["replay_s"] * 2 + 120
    for h, addr in ip.items():
        run_on(addr, sampler_cmd(h, total)).wait()
    print(f"미리 쓰기 최대 {prep_gib:.0f} GiB/대 → {start_at - time.time():.0f}초 뒤 출발, 재생 {plan['replay_s']:.0f}초")

    procs = {}
    for i, c in enumerate(clients):
        for j in range(args.procs):
            cmd = (f"cd {c['workdir']} && {args.prefix} {args.python or c['python']} driver.py --mode replay --schedule {remote_plan} --calibration {REMOTE_CAL} "
                   f"--client-index {i} --procs {args.procs} --proc-index {j} "
                   f"--data-dir {data_dir} --slots {args.slots} --start-at {start_at} "
                   f"--max-batch {args.max_batch} --quiet --out /tmp/scale-model-replay-{c['host']}-{j}.json"
                   + "".join(f" --backend-param {kv}" for kv in args.backend_param)
                   + (f" --adapter-json '{args.adapter_json}'" if args.adapter_json else "")
                   + (" --keep-prepop" if args.keep_prepop else "")
                   + (f" --l1-align {args.l1_align}" if args.l1_align else "")
                   + (" --load-rdonly" if args.load_rdonly else ""))
            procs[(c["host"], j)] = run_on(ip[c["host"]], cmd)
    kept = None
    if args.keep_prepop:          # 출발 직전의 파일 목록 = 미리 쓴 chunk. 끝나면 이 밖의 것만 지운다
        time.sleep(max(0, start_at - 3 - time.time()))
        kept = files_under(data_dir)
    failed = [f"{h}#{j}" for (h, j), p in procs.items() if p.wait() != 0]
    pids1 = C.storage_pids()
    if failed:
        raise SystemExit(f"driver 가 실패했다: {' '.join(failed)}")
    time.sleep(3)

    raw, meta = {}, {}
    for h, addr in ip.items():
        run_on(addr, "kill $(cat /tmp/hw_sampler.pid) 2>/dev/null; true").wait()
        text = run_on(addr, "cat /tmp/hw_sampler.out", stdout=subprocess.PIPE, text=True).communicate()[0]
        lines = []
        for l in text.splitlines():
            try:
                lines.append(json.loads(l))
            except json.JSONDecodeError:
                pass
        meta[h], raw[h] = lines[0], lines[1:]
        (out / f"raw-{h}.jsonl").write_text("\n".join(json.dumps(x) for x in lines))
    drv = {}
    for c in clients:                 # 프로세스들의 결과를 클라이언트 하나로 합친다
        parts = []
        for j in range(args.procs):
            path = f"/tmp/scale-model-replay-{c['host']}-{j}.json"
            parts.append(json.loads(Path(path).read_text() if ip[c["host"]] is None else
                                    subprocess.run(C.remote(c["addr"], f"cat {path}"), capture_output=True,
                                                   text=True, check=True).stdout))
        d = dict(parts[0], procs=args.procs)
        for k in ("records", "store_records"):
            d[k] = [r for x in parts for r in x[k]]
        for k in ("events", "prepopulate_chunks", "missed_loads", "max_outstanding_stores"):
            d[k] = sum(x[k] for x in parts)
        d["prepopulate_written"] = sum(x.get("prepopulate_written", x["prepopulate_chunks"]) for x in parts)
        for k, f in (("wall_s", max), ("prepopulate_s", max), ("start_gap_s", min)):
            d[k] = f(x[k] for x in parts)
        drv[c["host"]] = d
        (out / f"driver-{c['host']}.json").write_text(json.dumps(d))
    if kept is None:
        subprocess.run(["bash", "-c", f"find {data_dir} -mindepth 1 -delete"], check=True)
    else:
        with ThreadPoolExecutor(16) as ex:
            list(ex.map(lambda n: os.unlink(f"{data_dir}/{n}"), files_under(data_dir) - kept))
    prints = {h: (d["code"], d["l1_align"]) for h, d in drv.items()}
    if len(set(json.dumps(p) for p in prints.values())) > 1:
        raise SystemExit(f"클라이언트끼리 코드나 L1 정렬이 다르다 — 이 실행은 무효: {prints}")

    # 요약 — 재생 구간(출발 ~ 가장 늦게 끝난 클라이언트)
    wall = max(d["wall_s"] for d in drv.values())
    a, b = start_at, start_at + wall
    hw = {h: summarize(raw[h], meta[h], a, b) for h in ip}
    recs = [r for d in drv.values() for r in d["records"]]
    srecs = [r for d in drv.values() for r in d["store_records"]]
    e = {"load_gibps": plan["load_gibps"], "store_gibps": plan["store_gibps"], "store_chunks": plan["store_chunks"]}
    disk_r = sum(hw[s].get("disk_read_gibps", 0) for s in STORAGE)   # 구간이 몇 초뿐이면 샘플이 없다
    disk_w = sum(hw[s].get("disk_write_gibps", 0) for s in STORAGE)
    gib = plan["chunk_bytes"] / GIB
    loaded = sum(r["loaded"] for r in recs)
    res = {
        "plan": str(plan_path), "out": str(out), "level": plan.get("level"), "trace": plan["trace"], "speed": plan["speed"], "hosts": hosts,
        "stripe": args.stripe or cal["layout"]["default_stripe"], "slots": args.slots, "procs": args.procs, "max_batch": args.max_batch,
        "backend_param": args.backend_param, "adapter_json": args.adapter_json, "load_rdonly": args.load_rdonly,
        "data_dir": data_dir, "keep_prepop": args.keep_prepop,
        "storage_restart": {n: [p, pids1.get(n)] for n, p in pids0.items() if pids1.get(n) != p} or None,
        "prepopulate_written": sum(d["prepopulate_written"] for d in drv.values()),
        "replay_s_planned": plan["replay_s"], "wall_s": wall, "start_at": start_at,
        "start_gap_s": {h: d.get("start_gap_s") for h, d in drv.items()},
        "requests": len(recs), "missed_loads": sum(d["missed_loads"] for d in drv.values()),
        "late_s": {"p50": pct([r["late"] for r in recs], .5), "p99": pct([r["late"] for r in recs], .99),
                   "max": max(r["late"] for r in recs)},
        "load_s_per_request": {"p50": pct([r["load_s"] for r in recs if r["want"]], .5),
                               "p99": pct([r["load_s"] for r in recs if r["want"]], .99)},
        "store_s_per_batch": {"p50": pct([r["s"] for r in srecs], .5), "p99": pct([r["s"] for r in srecs], .99),
                              "max": max((r["s"] for r in srecs), default=None)},
        "store_failed": sum(1 for r in srecs if not r["ok"]),
        "max_outstanding_stores": max(d["max_outstanding_stores"] for d in drv.values()),
        "lookup": {"keys": sum(r["looked"] for r in recs), "found": sum(r["found"] for r in recs)},
        "planned_gibps": {"load": e["load_gibps"], "store": e["store_gibps"]},
        "achieved_gibps": {"load": loaded * gib / wall, "store": e["store_chunks"] * gib / wall},
        "disk_gibps": {"read": disk_r, "write": disk_w},
        "disk_write_over_store": disk_w / (e["store_chunks"] * gib / wall) if e["store_chunks"] else None,
        "hw": hw,
    }
    # 사용률: 디스크는 storage 합계 ÷ 천장, 링크는 가장 바쁜 NIC 한 방향 ÷ 선로 속도.
    # 분모는 plan 을 만들 때의 천장이 아니라 지금 calibration 의 천장 x 이 실행에 건 상한 배율이다
    c = util_ceilings(cal)
    ceil = {"disk_read": c["disk_read"] * args.disk_f, "disk_write": c["disk_write"] * args.disk_f,
            "link": c["link"] * args.net_f}
    rep, nodes, n = cal["replicas"]["value"], cal["storage_nodes"], len(hosts)
    ld, st = res["achieved_gibps"]["load"], res["achieved_gibps"]["store"]
    nic = lambda hs: max((max(hw[h]["roce_rx_gibps"], hw[h]["roce_tx_gibps"]) for h in hs if "error" not in hw[h]), default=0)
    res["util_ceilings_gibps"], res["disk_f"], res["net_f"] = ceil, args.disk_f, args.net_f
    res["util_planned"] = util_of(plan["load_gibps"], plan["store_gibps"], ceil, rep, nodes, n)
    res["util"] = {
        "disk_read": disk_r / ceil["disk_read"], "disk_write": disk_w / ceil["disk_write"],
        "client_link": max(ld, st) / n / ceil["link"],
        "storage_link": max(st * rep, ld + st * (rep - 1)) / nodes / ceil["link"],
        "client_link_wire": nic(hosts) / ceil["link"], "storage_link_wire": nic(STORAGE) / ceil["link"],
    }
    (out / "summary.json").write_text(json.dumps(res, ensure_ascii=False, indent=2))

    u, up = res["util"], res["util_planned"]
    if res["storage_restart"]:
        print(f"  !! 실행 중 storage 가 다시 떴다(node: [전, 뒤 PID]) {res['storage_restart']} — 이 실행은 무효")
    print(f"{plan['trace']} x{plan['speed']:g}  plan {plan['replay_s']:.0f}초 → 실제 {wall:.0f}초  request {len(recs)}  "
          f"못 찾은 load {res['missed_loads']}")
    print(f"  시작 늦음 p50 {res['late_s']['p50']:.2f}  p99 {res['late_s']['p99']:.2f}  최대 {res['late_s']['max']:.2f}초   "
          f"request 당 load p50 {res['load_s_per_request']['p50'] or 0:.2f}  p99 {res['load_s_per_request']['p99'] or 0:.2f}초")
    ss = res["store_s_per_batch"]
    if ss["p50"] is not None:
        print(f"  store 묶음 지연 p50 {ss['p50']:.2f}  p99 {ss['p99']:.2f}  최대 {ss['max']:.2f}초   "
              f"동시 store 최대 {res['max_outstanding_stores']}  실패 {res['store_failed']}   "
              f"lookup {res['lookup']['keys']} (찾음 {res['lookup']['found']})")
    print(f"  디스크 읽기 {up['disk_read']:.3f} → {u['disk_read']:.3f}  쓰기 {up['disk_write']:.3f} → {u['disk_write']:.3f}  "
          f"링크 클라이언트 {u['client_link']:.3f} (선로 {u['client_link_wire']:.3f})  "
          f"storage {u['storage_link']:.3f} (선로 {u['storage_link_wire']:.3f})")
    print(f"  load  plan {e['load_gibps']:.2f} → 달성 {res['achieved_gibps']['load']:.2f} GiB/s (디스크 읽기 {disk_r:.2f})")
    print(f"  store plan {e['store_gibps']:.2f} → 달성 {res['achieved_gibps']['store']:.2f} GiB/s (디스크 쓰기 {disk_w:.2f}, "
          f"x{res['disk_write_over_store'] or 0:.2f})")
    for h in ip:
        s = hw[h]
        if "error" in s:
            continue
        extra = (f" disk util {s['disk_util_pct']:.0f}%" if "disk_util_pct" in s and h in STORAGE else "")
        ev = {k: v for k, v in s["eth_events"].items() if "discard" in k or "pause" in k}
        print(f"  {h:9} cpu {s['cpu_total_pct']:4.1f}%{extra}  RoCE rx {s['roce_rx_gibps']:.2f} tx {s['roce_tx_gibps']:.2f}"
              + (f"  NIC {ev}" if ev else ""))


if __name__ == "__main__":
    main()
