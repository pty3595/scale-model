#!/usr/bin/env python3
"""replay 실행 하나를 plan 구간(출발 ~ 출발 + plan 길이) 안의 값으로 요약한다. tune.py 와 screen.py 가 쓴다.

summary.json 의 달성량 · 사용률은 남은 store 가 다 끝날 때까지의 시간으로 나눈 값이라, store 가 밀리면 load 와 읽기 사용률까지
낮게 나온다. 여기서는 구간 안만 본다.

  request 쪽  시작 늦음, request 당 load 시간(lookup 포함), 구간 안에 끝낸 load
  store 쪽    구간 안에 끝낸 store ÷ plan, 구간이 끝났을 때 남은 store(초), 못 찾은 load
  자원        디스크 읽기 · 쓰기, 링크(선로), 손실(storage NIC 수신 버림, 재전송, 받은 ÷ 쓸모)
  천장 대비   load · store ÷ 3FS 경로 천장(ceiling.py 가 잰 값), 디스크 읽기 · 쓰기 ÷ 디스크 천장([measured]) — 있을 때만

판정 (바꾸려면 verdict 만 고친다)
  request 쪽이 따라감  시작 늦음 p99 1초 미만
  store 쪽이 따라감    구간 안에 끝낸 store 가 plan 의 95% 이상, 못 찾은 load 1% 미만

    analyze.py <실행 폴더>...          한 줄씩
    analyze.py --json out.json <실행 폴더>...
"""
import json
import sys
from pathlib import Path

SM = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SM))
import cluster as C                          # noqa: E402
from hw_probe import STORAGE, summarize      # noqa: E402

GIB = 2**30
CAL = C.calibration()
CEIL = {k: CAL["ceilings_gibps"][CAL["util_denominator"][k]]["value"] for k in ("disk_read", "disk_write", "link")}   # 없으면 None
TFS = CAL.get("threefs_ceiling") or {}       # 3FS 경로 천장(ceiling.py)
LATE_MAX, STORE_MIN, MISS_MAX = 1.0, 0.95, 0.01      # store 95%: 같은 조건(coder 0.40) 6번이 95.8~97.6% 로 흩어져 97% 선은 같은 실행끼리 갈린다


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))] if xs else None


def rows(p):
    out = []
    for l in open(p):
        try:
            out.append(json.loads(l))
        except json.JSONDecodeError:
            pass
    return out[0], out[1:]


def metrics(run):
    run = Path(run)
    s = json.loads((run / "summary.json").read_text())
    plan = json.loads(Path(s["plan"]).read_text())
    gib = plan["chunk_bytes"] / GIB
    T = s["replay_s_planned"]
    a, b = s["start_at"], s["start_at"] + T
    drv = {h: json.loads((run / f"driver-{h}.json").read_text()) for h in s["hosts"]}
    recs = [r for d in drv.values() for r in d["records"]]
    srecs = [r for d in drv.values() for r in d["store_records"]]
    want = sum(r["want"] for r in recs)
    loaded = sum(r["loaded"] for r in recs)
    done_ld = sum(r["loaded"] for r in recs if r["due"] + r["late"] + r["load_s"] <= T)
    done_st = sum(r["n"] for r in srecs if r["ok"] and r["t"] + r["s"] <= T)
    sent_st = sum(r["n"] for r in srecs if r["t"] <= T)
    plan_ld, plan_st = plan["load_chunks"], plan["store_chunks"]
    lr = [r for r in recs if r["want"]]
    m = {
        "run": run.name, "trace": s["trace"], "speed": s["speed"], "variant": plan.get("variant", "full"),
        "replay_s": T, "wall_s": s["wall_s"], "tail_s": s["wall_s"] - T, "requests": len(recs),
        "disk_f": s.get("disk_f", 1.0), "net_f": s.get("net_f", 1.0),
        "plan_load": plan_ld * gib / T, "plan_store": plan_st * gib / T,
        "load": done_ld * gib / T, "store": done_st * gib / T,
        "load_ratio": done_ld / plan_ld if plan_ld else None, "store_ratio": done_st / plan_st if plan_st else None,
        "store_left_s": (sent_st - done_st) / (plan_st / T) if plan_st else None,     # 구간 끝에 남은 store 를 plan 속도로 쓰면 몇 초
        "missed": want - loaded, "missed_frac": (want - loaded) / want if want else None,
        "late_p50": pct([r["late"] for r in recs], .5), "late_p99": pct([r["late"] for r in recs], .99),
        "load_s_p50": pct([r["load_s"] for r in lr], .5), "load_s_p90": pct([r["load_s"] for r in lr], .9),
        "load_s_p99": pct([r["load_s"] for r in lr], .99),
        "lookup_s_p50": pct([r["lookup_s"] for r in recs if r["looked"]], .5),
        "lookup_s_p99": pct([r["lookup_s"] for r in recs if r["looked"]], .99),
        "store_s_p50": pct([r["s"] for r in srecs], .5), "store_s_p99": pct([r["s"] for r in srecs], .99),
        "store_failed": sum(1 for r in srecs if not r["ok"]), "max_outstanding_stores": s["max_outstanding_stores"],
        "stalled": {h: d["stalled"] for h, d in drv.items() if d.get("stalled")},      # driver 가 진행 없이 멈춰 끊은 실행(10-21)
        "start_gap_s": min(v for v in s["start_gap_s"].values() if v is not None),
        "prepopulate_written": s.get("prepopulate_written"),
        "storage_restart": s.get("storage_restart"),        # 실행 중 storage 가 다시 떴다(hot-update 가 풀린다, 10-24 apt 자동 업그레이드)
        "client_load": {h: sum(r["loaded"] for r in d["records"] if r["due"] + r["late"] + r["load_s"] <= T) * gib / T for h, d in drv.items()},
        "client_store": {h: sum(r["n"] for r in d["store_records"] if r["ok"] and r["t"] + r["s"] <= T) * gib / T for h, d in drv.items()},
        "client_plan_store": {h: sum(len(x) for e in ev for x in e[3]) * gib / T for h, ev in zip(s["hosts"], plan["events"])},
    }
    m["of_3fs_load"] = m["load"] / TFS["read_gibps"] if TFS else None
    m["of_3fs_store"] = m["store"] / TFS["write_gibps"] if TFS else None
    # 하드웨어: plan 구간 안
    hw = {}
    for h in dict.fromkeys(list(s["hosts"]) + list(STORAGE)):     # storage 가 클라이언트도 맡으면 장비 하나
        meta, raw = rows(run / f"raw-{h}.jsonl")
        hw[h] = summarize(raw, meta, a, b)
        if h in STORAGE:
            pre = [r for r in raw if a - 6 <= r["t"] <= a - 1]
            if len(pre) >= 2 and pre[-1].get("disk"):
                hw[h]["pre_write_gibps"] = (pre[-1]["disk"]["sect_w"] - pre[0]["disk"]["sect_w"]) * 512 / GIB / (pre[-1]["t"] - pre[0]["t"])
    ok = {h: v for h, v in hw.items() if "error" not in v}
    st, cl = [ok[h] for h in STORAGE if h in ok], [ok[h] for h in s["hosts"] if h in ok]
    if st and cl:
        dr, dw = sum(x["disk_read_gibps"] for x in st), sum(x["disk_write_gibps"] for x in st)
        ev = lambda xs, grp, key: sum(x[grp].get(key, 0) for x in xs)
        m.update({
            "disk_read": dr, "disk_write": dw,
            "util_disk_read": dr / (CEIL["disk_read"] * m["disk_f"]) if CEIL["disk_read"] else None,
            "util_disk_write": dw / (CEIL["disk_write"] * m["disk_f"]) if CEIL["disk_write"] else None,
            "disk_util_pct": max(x["disk_util_pct"] for x in st), "disk_queue": max(x["disk_avg_queue"] for x in st),
            "client_rx": sum(x["roce_rx_gibps"] for x in cl), "client_tx": sum(x["roce_tx_gibps"] for x in cl),
            "client_rx_max": max(x["roce_rx_gibps"] for x in cl), "client_tx_max": max(x["roce_tx_gibps"] for x in cl),
            "storage_rx_max": max(x["roce_rx_gibps"] for x in st), "storage_tx_max": max(x["roce_tx_gibps"] for x in st),
            "rx_over_useful": sum(x["roce_rx_gibps"] for x in cl) / m["load"] if m["load"] else None,
            "storage_rx_discards": ev(st, "eth_events", "rx_discards_phy"),
            "storage_oos": ev(st, "rdma_events", "out_of_sequence"), "client_oos": ev(cl, "rdma_events", "out_of_sequence"),
            "client_retrans_to": ev(cl, "rdma_events", "roce_adp_retrans_to"), "storage_retrans_to": ev(st, "rdma_events", "roce_adp_retrans_to"),
            "client_cpu_pct": max(x["cpu_total_pct"] for x in cl), "storage_cpu_pct": max(x["cpu_total_pct"] for x in st),
            "pre_write": sum(x.get("pre_write_gibps", 0) for x in st),
        })
    m.update(verdict(m))
    return m


def verdict(m):
    v = m["variant"]
    req = None if v == "store" else m["late_p99"] < LATE_MAX
    sto = None if v == "load" else (m["store_ratio"] >= STORE_MIN and (m["missed_frac"] or 0) < MISS_MAX)
    return {"req_ok": req, "store_ok": sto, "ok": all(x for x in (req, sto) if x is not None),
            "valid": m["start_gap_s"] > 0 and m["store_failed"] == 0 and not m.get("stalled") and not m.get("storage_restart")}


def line(m):
    f = lambda x, n=2: "  -  " if x is None else f"{x:.{n}f}"
    mark = lambda x: "-" if x is None else "O" if x else "X"
    return (f"{m['run']:38} x{m['speed']:<5.2f} load {f(m['load'])}/{f(m['plan_load'])}  store {f(m['store'])}/{f(m['plan_store'])} "
            f"({f(m['store_ratio'])}, 남음 {f(m['store_left_s'], 1)}초)  늦음 p99 {f(m['late_p99'])}  load p50 {f(m['load_s_p50'])} p99 {f(m['load_s_p99'])}  "
            f"못 찾음 {f(100 * m['missed_frac'] if m['missed_frac'] is not None else None, 1)}%  "
            f"디스크 r {f(m.get('disk_read'))} w {f(m.get('disk_write'))} GiB/s"
            + ("" if m.get("util_disk_read") is None else f" ({f(m['util_disk_read'])} · {f(m.get('util_disk_write'))})")
            + ("" if m["of_3fs_load"] is None else f"  3FS 천장 대비 load {f(m['of_3fs_load'])} store {f(m['of_3fs_store'])}")
            + f"  버림 {m.get('storage_rx_discards', 0):,}  "
            f"request {mark(m['req_ok'])} store {mark(m['store_ok'])}" + ("" if m["valid"] else "  !! 무효"))


if __name__ == "__main__":
    args = sys.argv[1:]
    out = None
    if args and args[0] == "--json":
        out, args = args[1], args[2:]
    ms = []
    for r in args:
        if (Path(r) / "summary.json").exists():
            ms.append(metrics(r))
            print(line(ms[-1]))
    if out:
        Path(out).write_text(json.dumps(ms, ensure_ascii=False, indent=1))
