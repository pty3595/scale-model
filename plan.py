#!/usr/bin/env python3
"""공개 trace 한 구간을 클라이언트별 replay plan 으로 바꾼다. replay 배속을 여러 개로 sweep 한다.

1. 캐시 시뮬레이터(cache_sim.py)로 trace 구간을 한 번 계산한다 — request 마다 3FS 에서 load·store 할 chunk.
   클라이언트 하나 = 인스턴스(LMCache MP 서버) 하나로, GPU prefix cache 와 L1 은 따로, 3FS 는 공유한다.
   request 는 L1 에 prefix 가 가장 길게 있는 클라이언트로 보낸다(비기면 지금까지 받은 chunk 가 가장 적은 곳).
   구간 앞의 request 도 캐시 상태를 만들기 위해 계산하되 event 는 구간 안만 낸다
2. 배속마다 plan 을 낸다. replay 시각 = (trace 시각 - 구간 시작) / 배속. 캐시 계산은 배속과 무관하므로 한 번만 한다.
   천장(clusters/<이름>.toml [measured])을 적어 두면 그 배속에서 예상되는 사용률도 붙인다
3. 가장 바쁜 클라이언트의 load·store 가 이 클러스터의 클라이언트 한계(calibration.json)를 넘는 배속은 빼고,
   max 는 그 한계까지의 배속이다. routing 이 prefix 를 따라가서 클라이언트마다 양이 다르다

event 하나 = request 하나: [replay 시각(초), lookup 할 key 들, load 할 key 들, store 묶음들]
  lookup: L1 prefix 뒤의 chunk 전부 (실제 LMCache 처럼 없는 key 도 묻는다 — adapter 가 3FS 에 stat 한다).
          서빙 설정의 lmcache.rules 가 8d7d2c4 면 chunk 전부(그 판의 LMCache 처럼)
          ad1d738 이면 GPU(vLLM APC)가 가진 앞부분 뒤의 chunk 전부(60538a0 부터의 LMCache 처럼)
  store 묶음: prefill 단계마다 하나 (8192 token 단계면 최대 32 chunk)
구간 안에서 3FS lookup 이 찾아야 하는데 구간 전에 저장됐어야 할 chunk 는 prepopulate 로 따로 낸다 (replay 전에 써 둔다).

    python3 plan.py alibaba-coder --out plans/coder
    python3 plan.py alibaba-coder --speeds 1,4,max --serving-cfg targets/<서빙 설정>.json --out plans/coder-big-l1
"""

import argparse
import copy
import json
from pathlib import Path

from traces import GIB, HERE, TRACES, prompt_tokens

KEY_BASE = 5_000_000      # 재생용 key idx 의 시작. load 데이터셋(0~)과 겹치지 않게
SERVING_BASE = HERE / "targets/serving-base.json"


def simulate(trace: str, start_min: float, minutes: float, clients: int, cfg: dict) -> dict:
    """trace 구간을 1배속 plan 으로 바꾼다 (event 시각 = 구간 시작부터의 trace 초)."""
    from cache_sim import CacheSim
    name, rel, block, tdiv = next(t for t in TRACES if t[0] == trace)
    reqs = sorted((json.loads(l) for l in open(HERE / rel)), key=lambda r: r["timestamp"])
    t0 = reqs[0]["timestamp"] / tdiv
    w0, w1 = start_min * 60, (start_min + minutes) * 60
    chunk_bytes = cfg["lmcache"]["chunk_size"] * cfg["model"]["kv_bytes_per_token"]
    max_len = cfg["model"]["max_model_len"] - 1
    gpu_blocks = cfg["vllm"]["kv_cache_memory_bytes"] // (cfg["vllm"]["block_size"] * cfg["model"]["kv_bytes_per_token"])
    sims = [CacheSim(cfg, gpu_blocks) for _ in range(clients)]
    l2, submitted = set(), set()
    for sm in sims:                                   # 3FS 는 모든 인스턴스가 공유한다
        sm.l2, sm.submitted = l2, submitted
    idx, grp = {}, {}                                 # chunk -> key 번호, 첫 chunk -> 대화 번호

    def key(c):
        if c not in idx:
            idx[c] = KEY_BASE + len(idx)
        return idx[c]

    events = [[] for _ in range(clients)]
    assigned = [0] * clients
    prepop = [set() for _ in range(clients)]
    stored_in_window = set()
    counts = {"L1": 0, "L2": 0, "store": 0, "dup": 0, "lost": 0, "gpu": 0}
    n_req = n_chunks = n_lookup = n_hit = 0
    for r in reqs:
        t = r["timestamp"] / tdiv - t0
        if t >= w1:
            break
        if r["input_length"] > max_len:              # 모델 최대 길이를 넘는 request 는 뺀다 (실제 엔진도 받지 않는다)
            continue
        toks = prompt_tokens(r, block)
        ck = sims[0].chunk_key(toks)
        if not ck:
            continue

        def cached_prefix(c):
            n = 0
            while n < len(ck) and ck[n] in sims[c].l1:
                n += 1
            return n
        cl = max(range(clients), key=lambda c: (cached_prefix(c), -assigned[c]))
        assigned[cl] += len(ck)
        l1_used = len(sims[cl].l1) / sims[cl].l1_cap          # 도착 때 L1 사용률 (prefetch 들이기 판정용)
        res = sims[cl].request(toks, 1, ck)
        labels = res["labels"]
        if t < w0:
            continue
        n_req += 1
        n_chunks += len(ck)
        for x in labels:
            counts[x] += 1
        s0, p1, p2 = res["s0"], res["p1"], res["p2"]
        lf = res["lookup_from"]                       # L2 lookup 이 묻는 첫 청크 (LMCache 판에 따라 p1 또는 0)
        n_lookup += len(ck) - lf
        n_hit += p2
        loads = [c for c, x in zip(ck, labels) if x == "L2"]
        stores = {c for c, x in zip(ck, labels) if x == "store"}
        batches = [[key(c) for c in b if c in stores] for b in res["store_batches"]]
        for c in ck[s0 + p1:s0 + p1 + p2]:            # lookup 이 찾는 chunk (L1 에 남아 있어 읽지 않는 것도)
            if c not in stored_in_window:
                prepop[cl].add(key(c))
        stored_in_window.update(stores)
        if lf < len(ck) or stores:
            # 4 번째는 대화 번호 — replay 가 한 클라이언트의 event 를 프로세스 여럿에 나눌 때 같은 대화를 한 프로세스에 둔다
            # 5 번째는 도착 때 L1 사용률
            events[cl].append([t - w0, [key(c) for c in ck[lf:]], [key(c) for c in loads], [b for b in batches if b],
                               grp.setdefault(ck[0], len(grp)), round(l1_used, 4)])

    seen = set()
    for p in prepop:                                  # 같은 chunk 를 두 클라이언트가 미리 쓰지 않게
        p -= seen
        seen |= p
    return {
        "trace": name, "window_min": [start_min, start_min + minutes], "clients": clients, "rules": cfg["lmcache"].get("rules", "a44d5e5"),
        # LMCache 의 L2 prefetch 들이기: 8d7d2c4 까지는 동시 8개(replay --slots), a6accee6(10-21)부터는 L1 바이트 기준(replay 가 따라 한다)
        "admission": "watermark" if cfg["lmcache"].get("rules") == "ad1d738" else "slots",
        "l1_bytes": cfg["lmcache"]["l1_size_gb"] * 2**30, "prefetch_watermark": cfg["lmcache"].get("l2_prefetch_inflight_watermark", 0.5),
        "serving_cfg": cfg.get("name"), "chunk_bytes": chunk_bytes, "chunk_tokens": cfg["lmcache"]["chunk_size"],
        "requests": n_req, "chunks": n_chunks, "labels": counts,
        "load_chunks": counts["L2"], "store_chunks": counts["store"],
        "lookup_chunks": n_lookup, "lookup_misses": n_lookup - n_hit,
        "prepopulate_chunks": sum(len(p) for p in prepop),
        "events": events, "prepopulate": [sorted(p) for p in prepop],
    }


def at_speed(base: dict, speed: float, cal: dict) -> dict:
    """1배속 plan 을 배속 speed 로 옮기고, 이 클러스터에서 예상되는 사용률을 붙인다(천장이 없으면 None)."""
    plan = copy.deepcopy(base)
    replay_s = (base["window_min"][1] - base["window_min"][0]) * 60 / speed
    gib = base["chunk_bytes"] / GIB
    load, store = base["load_chunks"] * gib / replay_s, base["store_chunks"] * gib / replay_s
    ceil = util_ceilings(cal)
    rep, nodes, n = cal["replicas"]["value"], cal["storage_nodes"], base["clients"]
    for ev in plan["events"]:
        for e in ev:
            e[0] /= speed
    per_client = {"load": [sum(len(e[2]) for e in ev) * gib / replay_s for ev in base["events"]],
                  "store": [sum(len(b) for e in ev for b in e[3]) * gib / replay_s for ev in base["events"]]}
    plan.update({
        "speed": speed, "replay_s": replay_s, "load_gibps": load, "store_gibps": store, "client_gibps": per_client,
        "util_ceilings_gibps": ceil,
        "util": util_of(load, store, ceil, rep, nodes, n) if all(ceil.values()) else None,
    })
    return plan


def util_of(load: float, store: float, ceil: dict, rep: int, nodes: int, n: int) -> dict:
    """load·store(GiB/s, payload)가 만드는 사용률. storage 링크: 받는 쪽 = store x 복제 수, 보내는 쪽 = load + chain 전달"""
    return {
        "disk_read": load / ceil["disk_read"],
        "disk_write": store * rep / ceil["disk_write"],
        "client_link": max(load, store) / n / ceil["link"],
        "storage_link": max(store * rep, load + store * (rep - 1)) / nodes / ceil["link"],
    }


def util_ceilings(cal: dict) -> dict:
    d = cal["util_denominator"]
    return {k: cal["ceilings_gibps"][d[k]]["value"] for k in ("disk_read", "disk_write", "link")}


def sweep(base: dict, cal: dict, speeds: list[str]) -> list[dict]:
    """배속마다 plan. 클라이언트 한계를 넘는 배속은 이유와 함께 뺀다. max 는 그 한계까지의 배속"""
    one = at_speed(base, 1.0, cal)
    if one["load_gibps"] <= 0:
        raise SystemExit("이 구간은 3FS load 가 없다. 다른 구간을 고른다.")
    inf = float("inf")      # 클러스터 설정에 클라이언트 한계가 없으면(None) 자르지 않는다
    load_wall, store_wall = cal["client_wall"]["paced_gibps"] or inf, cal["store_wall"]["overlap_gibps"] or inf
    ld1, st1 = max(one["client_gibps"]["load"]), max(one["client_gibps"]["store"])   # 가장 바쁜 클라이언트, 1배속
    max_speed = min(load_wall / ld1, store_wall / st1 if st1 else inf)
    out = []
    for sp in speeds:
        name = sp if sp == "max" else f"x{float(sp):g}"
        if sp == "max":
            if max_speed == inf:
                out.append({"name": name, "skipped": "클라이언트 한계를 모른다 — [measured] client_load_wall_gibps · client_store_wall_gibps 나 --load-wall · --store-wall"})
                continue
            speed = max_speed
        else:
            speed = float(sp)
            if speed > max_speed * 1.0001:
                need = []
                if ld1 * speed > load_wall:
                    need.append(f"가장 바쁜 클라이언트 load {ld1 * speed:.2f} > {load_wall} GiB/s")
                if st1 * speed > store_wall:
                    need.append(f"가장 바쁜 클라이언트 store {st1 * speed:.2f} > {store_wall} GiB/s")
                out.append({"name": name, "skipped": ", ".join(need)})
                continue
        plan = at_speed(base, speed, cal)
        plan["name"] = name
        out.append(plan)
    return out


def describe(plan: dict) -> str:
    u = plan["util"]
    return (f"  {plan['name']:>6}  x{plan['speed']:.3g}  replay {plan['replay_s']:.0f}초  "
            f"load {plan['load_gibps']:.2f} store {plan['store_gibps']:.2f} GiB/s "
            f"(가장 바쁜 클라이언트 {max(plan['client_gibps']['load']):.2f} · {max(plan['client_gibps']['store']):.2f})  "
            + (f"디스크 읽기 {u['disk_read']:.3f} 쓰기 {u['disk_write']:.3f}  "
               f"링크 클라이언트 {u['client_link']:.3f} storage {u['storage_link']:.3f}" if u else "사용률 — 천장이 없다"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("trace", choices=[t[0] for t in TRACES])
    ap.add_argument("--window-start", type=float, default=30, help="replay 할 trace 구간 시작(분)")
    ap.add_argument("--window-min", type=float, default=3, help="구간 길이(분)")
    ap.add_argument("--speeds", default="0.5,1,2,3,4,max",
                    help="replay 배속, 쉼표로. max 는 클라이언트 한계까지")
    ap.add_argument("--clients", type=int, default=None, help="기본: 이 클러스터의 클라이언트 수(clusters/*.toml)")
    ap.add_argument("--serving-cfg", default=str(SERVING_BASE),
                    help="서빙 설정 — 모델·GPU KV·L1 크기·정책 (기본: targets/serving-base.json)")
    ap.add_argument("--calibration", default=None, help="기본: calibration.json 에 이 클러스터의 천장을 얹은 것(cluster.py)")
    ap.add_argument("--load-wall", type=float, default=None,
                    help="클라이언트 한 대 load 한계 GiB/s (기본: calibration 의 dram 값). dram_zc 로 replay 하면 올린다")
    ap.add_argument("--store-wall", type=float, default=None,
                    help="클라이언트 한 대 store 한계 GiB/s (기본: calibration 의 dram 값)")
    ap.add_argument("--out", required=True, help="plan 폴더. 배속마다 파일 하나")
    args = ap.parse_args()

    cfg = json.loads(Path(args.serving_cfg).read_text())
    import cluster as C
    cal = json.loads(Path(args.calibration).read_text()) if args.calibration else C.calibration()
    args.clients = args.clients or len(C.CLIENTS)
    if args.load_wall:
        cal["client_wall"]["paced_gibps"] = args.load_wall
    if args.store_wall:
        cal["store_wall"]["overlap_gibps"] = args.store_wall
    base = simulate(args.trace, args.window_start, args.window_min, args.clients, cfg)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    lb = base["labels"]
    print(f"{base['trace']} {base['window_min'][0]:g}~{base['window_min'][1]:g}분  request {base['requests']}  "
          f"chunk {base['chunks']} — L1 {lb['L1']} · 3FS load {lb['L2']} · store {lb['store']} · 3FS IO 없음 {lb['lost'] + lb['dup'] + lb.get('gpu', 0)}  "
          f"3FS lookup {base['lookup_chunks']} (없는 key {base['lookup_misses']})  미리 쓸 chunk {base['prepopulate_chunks']}  "
          f"store÷load {base['store_chunks'] / max(1, base['load_chunks']):.2f}")   # 1 을 넘으면 store 벽에 먼저 걸리기 쉽다(10-24 coder 60분 1.26)
    index = []
    for p in sweep(base, cal, args.speeds.split(",")):
        if "skipped" in p:
            print(f"  {p['name']:>6}  뺌 — {p['skipped']}")
            index.append(p)
            continue
        (out / f"{p['name']}.json").write_text(json.dumps(p))
        print(describe(p))
        index.append({"name": p["name"], "file": f"{p['name']}.json", "speed": p["speed"], "util": p["util"]})
    (out / "index.json").write_text(json.dumps({"trace": base["trace"], "window_min": base["window_min"],
                                                "serving_cfg": base["serving_cfg"], "plans": index},
                                               ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
