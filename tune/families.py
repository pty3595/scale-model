#!/usr/bin/env python3
"""sweep 용 plan 묶음(family). family 하나 = trace 구간 하나 x 메모리 설정 하나를 1배속으로 계산한 것(base.json).
캐시 계산은 배속과 무관하므로 family 마다 한 번만 하고, 배속 plan 은 base 에서 그때그때 만든다(plan.at_speed).

  mem 이름 g<GPU KV GiB>-l<L1 GiB>   — serving-base.json 에서 vllm.kv_cache_memory_bytes · lmcache.l1_size_gb 만 바꾼다
  variant  full   trace 그대로 (lookup · load · store)
           load   store 를 뺀다. 구간 안에서 store 될 chunk 도 미리 써 둬서 load 는 trace 와 같다 — 읽기 경로만의 한계
           store  lookup · load 를 뺀다 — 쓰기 경로만의 한계

    families.py make <trace> <시작 분> <길이 분> <mem> [...]     base.json 을 만든다 (있으면 건너뜀)
    families.py table                                           만들어 둔 family 의 1배속 요구량 표
"""
import copy
import json
import sys
from pathlib import Path

SM = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SM))
import cluster as C                   # noqa: E402
import plan as P                      # noqa: E402
from traces import GIB                # noqa: E402

ROOT = SM / "plans/hf3fs-sweep-20261012"
SHORT = {"alibaba-coder": "coder", "alibaba-chat(A)": "chata", "alibaba-api(B)": "apib", "alibaba-thinking": "thinking",
         "mooncake-conversation": "mconv", "mooncake-toolagent": "mtool"}
CAL = C.calibration()


def fam_name(trace, start, minutes, mem):
    return f"{SHORT[trace]}-{start:g}m{minutes:g}-{mem}"


def serving_cfg(mem):
    g, l1 = (int(x[1:]) for x in mem.split("-"))
    cfg = json.loads((SM / "targets/serving-base.json").read_text())
    cfg["vllm"]["kv_cache_memory_bytes"] = g * GIB
    cfg["lmcache"]["l1_size_gb"] = l1
    cfg["name"] = f"serving-base, GPU KV {g} GiB, L1 {l1} GiB"
    return cfg


def make(trace, start, minutes, mem):
    d = ROOT / fam_name(trace, start, minutes, mem)
    if not (d / "base.json").exists():
        d.mkdir(parents=True, exist_ok=True)
        base = P.simulate(trace, start, minutes, len(C.CLIENTS), serving_cfg(mem))
        (d / "base.json").write_text(json.dumps(base))
    return d


def variant(base, kind):
    """base(1배속 plan)에서 읽기만 · 쓰기만 plan 을 만든다."""
    if kind == "full":
        return base
    b = copy.deepcopy(base)
    if kind == "load":
        for c, ev in enumerate(b["events"]):
            extra = {k for e in ev for bt in e[3] for k in bt}
            b["prepopulate"][c] = sorted(set(b["prepopulate"][c]) | extra)
            for e in ev:
                e[3] = []
        b["store_chunks"] = 0
    elif kind == "store":
        for ev in b["events"]:
            for e in ev:
                e[1], e[2] = [], []
        b["events"] = [[e for e in ev if e[3]] for ev in b["events"]]
        b["prepopulate"] = [[] for _ in b["prepopulate"]]
        b["load_chunks"] = 0
    b["prepopulate_chunks"] = sum(len(p) for p in b["prepopulate"])
    return b


def speed_plan(fam_dir, kind, speed):
    """배속 plan 파일을 만들어 경로를 돌려준다(있으면 그대로)"""
    out = Path(fam_dir) / f"{kind}-x{speed:g}.json"
    base = json.loads((Path(fam_dir) / "base.json").read_text())
    if base["clients"] != len(C.CLIENTS):
        raise SystemExit(f"{fam_dir} 는 클라이언트 {base['clients']}대로 계산했다 — 이 클러스터는 {len(C.CLIENTS)}대")
    if not out.exists():
        p = P.at_speed(variant(base, kind), speed, CAL)
        p["name"], p["variant"] = f"x{speed:g}", kind
        out.write_text(json.dumps(p))
    return out


def demand(fam_dir):
    b = json.loads((Path(fam_dir) / "base.json").read_text())
    gib = b["chunk_bytes"] / GIB
    win = (b["window_min"][1] - b["window_min"][0]) * 60
    ld, st = b["load_chunks"] * gib / win, b["store_chunks"] * gib / win
    per_ld = [sum(len(e[2]) for e in ev) for ev in b["events"]]
    per_st = [sum(len(x) for e in ev for x in e[3]) for ev in b["events"]]
    return {"family": Path(fam_dir).name, "requests": b["requests"], "req_per_s": b["requests"] / win,
            "load_gibps": ld, "store_gibps": st, "ratio": ld / st if st else float("inf"),
            "lookup_keys": b["lookup_chunks"], "l1_chunks": b["labels"]["L1"], "chunks": b["chunks"],
            "prepop_gib": [len(p) * gib for p in b["prepopulate"]], "store_gib": b["store_chunks"] * gib,
            "busiest_load": max(per_ld) / max(1, sum(per_ld)), "busiest_store": max(per_st) / max(1, sum(per_st)),
            "read_util_1x": ld / P.util_ceilings(CAL)["disk_read"] if P.util_ceilings(CAL)["disk_read"] else None}


if __name__ == "__main__":
    if sys.argv[1] == "make":
        a = sys.argv[2:]
        for i in range(0, len(a), 4):
            d = make(a[i], float(a[i + 1]), float(a[i + 2]), a[i + 3])
            print(d.name, "ok", flush=True)
    else:
        print("| family | request | req/s | load GiB/s | store GiB/s | load÷store | L1 적중 chunk % | 미리 쓰기 GiB(대별) | 구간 store GiB | 가장 바쁜 대 load·store 몫 | 1배속 디스크 읽기 사용률 |")
        print("|---|---|---|---|---|---|---|---|---|---|---|")
        for d in sorted(ROOT.iterdir()):
            if (d / "base.json").exists():
                x = demand(d)
                print(f"| {x['family']} | {x['requests']} | {x['req_per_s']:.1f} | {x['load_gibps']:.2f} | {x['store_gibps']:.2f} | {x['ratio']:.2f} | "
                      f"{100 * x['l1_chunks'] / x['chunks']:.0f} | {' · '.join(f'{g:.0f}' for g in x['prepop_gib'])} | {x['store_gib']:.0f} | "
                      f"{100 * x['busiest_load']:.0f}% · {100 * x['busiest_store']:.0f}% | "
                      + ("-" if x["read_util_1x"] is None else f"{100 * x['read_util_1x']:.1f}%") + " |")
