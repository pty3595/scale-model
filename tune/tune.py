#!/usr/bin/env python3
"""설정 sweep 도구 — 그때의 클러스터 상태와 adapter 버전에서 소프트웨어 설정의 최적값을 같은 replay 로 찾고,
replay 가 그 버전의 실제 IO 를 아직 내는지 chunk 대조로 확인한다. 패치가 오면 verify → (어긋나면 시뮬레이터 규칙 고침) → sweep.

  tune.py verify VER                     실제 vLLM + 계측 LMCache 3대 대 시뮬레이터, 데워진 구간 chunk 대조 (약 1.5시간)
  tune.py sweep VER [--knobs a,b] [--settings x,y] [--trace T] [--model M]
                                         adapter 설정마다 → 가장 좋은 것에 FUSE · layout 설정마다, 따라가는 최대 배속
                                         (trace 구간 · 모델의 KV 크기를 바꿔 같은 sweep — WORKLOADS · MODELS)
  tune.py table                          결과 표

버전 · 설정 목록은 VERSIONS · SETTINGS. 실행마다 다른 사람의 작업이 없는지 보고, 끝나면(어떻게 끝나든) FUSE · storage 를 되돌린다.
클러스터 고유 값과 허락 범위는 clusters/<이름>.toml(cluster.py). 허락이 없는 쪽을 바꾸는 설정은 건너뛴다. 재시작이 필요한 설정은 SETTINGS 에 없다.
시작 때 클러스터가 되돌릴 곳(템플릿 사본 · [home])에 있는지 보고, 아니면 멈춘다.
replay 의 IO 는 버전의 LMCache 규칙(cache_sim.py 의 lmcache.rules)으로 만든 plan 을 쓴다.
LMCache 빌드는 클라이언트마다 $HOME 아래 같은 이름으로 둔다(VERSIONS 의 build · HF_PY · prefix).
판정은 analyze.py, plan 묶음은 families.py, 클러스터 층 설정은 settings.py(results/ 의 10-12 · 10-18 코드에서 옮겼다).
"""
import argparse
import collections
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

SM = Path(__file__).resolve().parents[1]
for p in (SM, SM / "validate", Path(__file__).resolve().parent):
    sys.path.insert(0, str(p))
import analyze as A            # noqa: E402  판정
import cluster as C            # noqa: E402  클러스터 고유 값
import families as F           # noqa: E402
import plan as P               # noqa: E402
import settings as S           # noqa: E402  FUSE · storage 적용 · 확인 · 되돌리기
from guard import busy         # noqa: E402
from replay import admin, ensure_layout     # noqa: E402

OUT = SM / "results/tune"
DATA = C.data("tune")
GRID = [0.03, 0.04, 0.05, 0.07, 0.10, 0.14, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]
MEM = "g24-l32"
MIN_FREE = 0.15                      # 3FS 여유가 이보다 적으면 실행을 시작하지 않는다
HF_PY = "$HOME/venv-lmcache-hf3fs/bin/python"
# trace 구간: 이름 → (trace, 시작 분, 길이 분). 구간은 hf3fs-sweep-20261012 의 family 와 같다
WORKLOADS = {"coder": ("alibaba-coder", 30, 3), "apib": ("alibaba-api(B)", 30, 5), "chata": ("alibaba-chat(A)", 30, 9),
             "coder60": ("alibaba-coder", 60, 3)}   # 10-23 주말: 같은 trace 의 다른 구간 — 고른 설정이 구간 하나에만 맞는지 본다
# 모델: 이름 → token 당 KV bytes (bf16, 모델 config 로 계산). key 하나 = chunk 256 token
MODELS = {"qwen3-14b": 163840,          # 40 layer x KV head 8 x 128 x (K, V) x 2 B = key 40 MiB (serving-base.json)
          "ds-v3": 70272,               # MLA 61 layer x (512 + 64) x 2 B = key 17 MiB
          "llama-70b": 327680}          # 80 layer x KV head 8 x 128 x (K, V) x 2 B = key 80 MiB (TP 1)


def hf3fs(nd, store16):
    bp = "{dir}" if nd == 1 else ",".join(f"{{dir}}/p{i}" for i in range(nd))
    ops = ',"per_op_workers":{"lookup":2,"retrieve":16,"store":16}' if store16 else ""
    return '{"type":"hf3fs","mount_point":"' + C.MOUNT + '","base_paths":"' + bp + '","num_workers":4,"enable_key_buffer":false' + ops + "}"


def hf3fs_doc():
    """hf3fs.rst 의 예시 그대로(폴더 2 · num_workers 4 · lookup 2 · retrieve 16 · store 4). key buffer 만 끈다 — 켜면 노드끼리 서로의 chunk 를 못 찾는다"""
    return ('{"type":"hf3fs","mount_point":"' + C.MOUNT + '","base_paths":"{dir}/p0,{dir}/p1","num_workers":4,"enable_key_buffer":false,'
            '"per_op_workers":{"lookup":2,"retrieve":16,"store":4}}')


def native(nw, mem="dram", dio="true"):
    return ('{"type":"nixl_native","backend":"HF3FS","backend_params":{"file_path":"{dir}","use_direct_io":"' + dio + '",'
            '"mount_point":"' + C.MOUNT + '","mem_config":"' + mem + '"},"num_workers":' + str(nw) + "}")


AT = "PYTHONPATH=$HOME/LMCache-hf3fs-atomic"
# 버전: 빌드(클라이언트 세 대에 같은 경로), LMCache 규칙, 실제 대조에 쓸 node_up.sh 의 ADAPTER, 처음 잴 단계, adapter 설정(knob)마다 replay 인자
VERSIONS = {
    "dyn": dict(name="nixl_store_dynamic", build="~/LMCache 8d7d2c4", rules="8d7d2c4", real="nixl_store_dynamic", start=0.07, knobs={
        "dram": dict(nd=1),                                                   # calibration 의 adapter (문서 예시 아님 — 문서는 mem_config 를 안 준다)
        "auto": dict(nd=1, bp=["mem_config=auto"]),                           # nixl.rst 예시 = mem_config 없음 = plugin 기본값 auto
        "dram_zc": dict(nd=1, bp=["mem_config=dram_zc"], prefix="LD_PRELOAD=/tmp/pwrite_full.so"),   # NIXL 2 GiB 버그 shim
        "dram_zc-a4k": dict(nd=1, bp=["mem_config=dram_zc"], prefix="LD_PRELOAD=/tmp/pwrite_full.so", l1a=4096),   # L1 정렬 LMCache 기본(4 KiB)
        "dram_zc-a2m": dict(nd=1, bp=["mem_config=dram_zc"], prefix="LD_PRELOAD=/tmp/pwrite_full.so", l1a=2097152),
        "dram_zc-nodio": dict(nd=1, bp=["mem_config=dram_zc", "use_direct_io=false"], prefix="LD_PRELOAD=/tmp/pwrite_full.so")}),   # O_DIRECT 끔   # 2 MiB — 8 MiB 가 나은 까닭이 huge page 경계인가
    "nat": dict(name="nixl_native", build="~/LMCache-native 8eb36fa", rules="8d7d2c4", real=None, start=0.07, knobs={
        "w4": dict(nd=1, aj=native(4), prefix="PYTHONPATH=$HOME/LMCache-native"),
        "w16": dict(nd=1, aj=native(16), prefix="PYTHONPATH=$HOME/LMCache-native"),
        "w16-a4k": dict(nd=1, aj=native(16), prefix="PYTHONPATH=$HOME/LMCache-native", l1a=4096),   # L1 정렬 LMCache 기본(4 KiB)
        # 10-22: nixl_store_dynamic 은 dram → dram_zc 가 0.07 → 0.20 이었다. native 는 dram 으로만 쟀다
        "w16-zc": dict(nd=1, aj=native(16, "dram_zc"), prefix="LD_PRELOAD=/tmp/pwrite_full.so PYTHONPATH=$HOME/LMCache-native"),
        "w16-zc-a4k": dict(nd=1, aj=native(16, "dram_zc"), prefix="LD_PRELOAD=/tmp/pwrite_full.so PYTHONPATH=$HOME/LMCache-native", l1a=4096),
        "w16-nodio": dict(nd=1, aj=native(16, dio="false"), prefix="PYTHONPATH=$HOME/LMCache-native")}),   # O_DIRECT 끔
    # 10-23 사용자: hf3fs 는 최신 버전(원자적 쓰기 패치) 하나로 잰다. 예전 버전(~/LMCache-hf3fs ecb031a, 키 "h")의 결과는 results/tune/h* 와 NOTE 에 남는다
    "a": dict(name="hf3fs", build="~/LMCache-hf3fs-atomic (6e69015 connector)", rules="8d7d2c4", real="hf3fs-atomic", start=0.20, knobs={
        "w4-d1": dict(nd=1, aj=hf3fs(1, False), py=HF_PY, prefix=AT),
        "s16-d1": dict(nd=1, aj=hf3fs(1, True), py=HF_PY, prefix=AT),
        "s16-d16": dict(nd=16, aj=hf3fs(16, True), py=HF_PY, prefix=AT),
        "s16-d16-a4k": dict(nd=16, aj=hf3fs(16, True), py=HF_PY, prefix=AT, l1a=4096),   # L1 정렬 LMCache 기본(4 KiB)
        # LMCache prefetch 동시 수(--l2-prefetch-max-in-flight, 기본 8) = replay --slots. 같은 폴더를 함께 쓴다(data_dir 는 knob 이름마다)
        "s16-d16-sl4": dict(nd=16, aj=hf3fs(16, True), py=HF_PY, prefix=AT, slots=4),
        "s16-d16-sl16": dict(nd=16, aj=hf3fs(16, True), py=HF_PY, prefix=AT, slots=16),
        "s16-d1-sl4": dict(nd=1, aj=hf3fs(1, True), py=HF_PY, prefix=AT, slots=4),
        "s16-d1-sl16": dict(nd=1, aj=hf3fs(1, True), py=HF_PY, prefix=AT, slots=16),
        "doc": dict(nd=2, aj=hf3fs_doc(), py=HF_PY, prefix=AT)}),             # hf3fs.rst 예시
}
# 클러스터 설정 (기준에서 하나씩). chunk 는 재생 폴더의 3FS layout
SETTINGS = {
    # base = 실험 기준 settings.REF(clusters/<이름>.toml [ref], 측정으로 고른 값, 템플릿과 따로 — 읽기 12/3 · 4MB 는 09-14-15 의 최적화). src = 3FS 소스 기본값(읽기 32/8 · 4MB) — 보고의 출발점(HANDOFF 기준선)
    "base": {}, "src": dict(read="32/8"), "b1m": dict(batch="1MB"), "r32-1m": dict(read="32/8", batch="1MB"),
    "r6": dict(read="6/2"), "rts": dict(rdma="true", rts="on"), "c512k": dict(read="32/8", batch="512KB", chunk=524288),
    "c4m": dict(chunk=4194304),
    "ch512k": dict(chunk=524288),          # chunk 만. c512k 는 읽기 32/8 · 묶음 512KB 도 함께 바꿔 chunk 효과로 못 읽는다
    "w32": dict(write="32/8"),             # FUSE 쓰기 admission 소스 기본값 — FUSE 재시작 뒤 FUSE_WRITE=32/8 로만 돈다
    "src-w32": dict(read="32/8", write="32/8"),   # 읽기 · 쓰기 모두 소스 기본값 = 참 기준선(문서 예시 adapter 와 함께). FUSE_WRITE=32/8 로만 돈다
}


# --- plan ---
def family(rules, wl="coder", model="qwen3-14b", ev=None):
    """trace 구간 하나 x 메모리 설정 x LMCache 규칙 x 모델(x L1 비우기 문턱 · 비율). a44d5e5 · 기본 모델은 예전 sweep 의 plan 그대로."""
    trace, start, minutes = WORKLOADS[wl]
    if rules == "a44d5e5" and model == "qwen3-14b" and not ev:
        return F.make(trace, start, minutes, MEM)
    d = SM / "plans/tune" / (f"{F.fam_name(trace, start, minutes, MEM)}-{rules}" + ("" if model == "qwen3-14b" else f"-{model}")
                             + (f"-ev{ev.replace('/', '-')}" if ev else ""))
    if not (d / "base.json").exists():
        cfg = F.serving_cfg(MEM)
        cfg["lmcache"]["rules"] = rules
        cfg["model"]["kv_bytes_per_token"] = MODELS[model]
        if ev:                                             # LMCache --eviction-trigger-watermark / --eviction-ratio (기본 0.8 / 0.2)
            w, r = ev.split("/")
            cfg["lmcache"]["eviction_trigger_watermark"], cfg["lmcache"]["eviction_ratio"] = float(w), float(r)
        d.mkdir(parents=True, exist_ok=True)
        (d / "base.json").write_text(json.dumps(P.simulate(trace, start, minutes, len(C.CLIENTS), cfg)))
    return d


# --- 재생 폴더 ---
def data_dir(ver, knob, chunk, wm=""):
    """버전 · knob · chunk 크기(· trace · 모델)마다 폴더 하나. 미리 쓴 chunk 를 단계 · 클러스터 설정이 바뀌어도 다시 쓴다(--keep-prepop)."""
    nd = VERSIONS[ver]["knobs"][knob]["nd"]
    d = f"{DATA}/{ver}{wm}-{knob}" + ("" if chunk is None else f"-c{chunk >> 10}k")
    for x in [d] + ([f"{d}/p{i}" for i in range(nd)] if nd > 1 else []):
        if chunk is None:
            os.makedirs(x, exist_ok=True)
            os.chmod(x, 0o777)
        else:
            ensure_layout(x, 8, chunk)             # 하위 폴더에도 layout 을 직접 건다
    lay = {l.split()[0]: l.split()[-1] for l in admin(f"stat --display-layout {C.rel(d)}").splitlines()
           if l.startswith("Layout-")}
    if lay.get("Layout-ChunkSize") != str(chunk or 1048576):
        raise SystemExit(f"{d} chunk {lay.get('Layout-ChunkSize')} (원한 것 {chunk or 1048576})")
    return d


# --- sweep ---
class Sweep:
    def __init__(self, ver, wl="coder", model="qwen3-14b", ev=None):
        self.ver, self.v = ver, VERSIONS[ver]
        self.fam = family(self.v["rules"], wl, model, ev)
        self.wm = ("" if wl == "coder" else f"-{wl}") + ("" if model == "qwen3-14b" else f"-{model}") + (f"-ev{ev.replace('/', '-')}" if ev else "")
        self.out = OUT / f"{ver}{self.wm}"
        (self.out / "runs").mkdir(parents=True, exist_ok=True)
        self.res_path = self.out / "result.json"
        self.res = json.loads(self.res_path.read_text()) if self.res_path.exists() else {}
        b = json.loads((self.fam / "base.json").read_text())
        S.log(f"plan {self.fam.name}: store÷load {b['store_chunks'] / max(1, b['load_chunks']):.2f}"
              " — 1 을 넘으면 store 벽에 먼저 걸리기 쉬워 store 쪽 설정부터 본다(10-24 coder 60분)")

    def run(self, tag, c, d, level):
        out = self.out / "runs" / f"{tag}-u{level:.2f}"
        if (out / "summary.json").exists():
            return out
        dl = Path(__file__).with_name("deadline")      # 있으면(epoch 초) 그 전에 끝나지 않을 실행은 시작하지 않는다
        if dl.exists() and time.time() + 8 * 60 > float(dl.read_text()):
            raise SystemExit(f"마감 {time.strftime('%m-%d %H:%M', time.localtime(float(dl.read_text())))} — 실행을 더 시작하지 않는다")
        why = busy()
        if why:
            raise SystemExit("다른 작업이 보여 멈춘다: " + "; ".join(why))
        fs = os.statvfs(C.MOUNT)                       # 10-21: 지운 파일이 회수되지 않아 3FS 가 98% 찼다
        if fs.f_bavail / fs.f_blocks < MIN_FREE:
            raise SystemExit(f"3FS 여유 {fs.f_bavail / fs.f_blocks:.1%} < {MIN_FREE:.0%} — GC 가 회수하는지 먼저 본다 (gcwatch.sh)")
        plan = F.level_plan(self.fam, "full", level)
        cmd = (f"cd {SM} && exec python3 replay.py {plan} --out {out} --procs 1 --slots {c.get('slots', 8)} --data-dir {d} --keep-prepop"
               + (f" --adapter-json '{c['aj']}'" if c.get("aj") else "")
               + (f" --python '{c['py']}'" if c.get("py") else "")
               + (f" --prefix '{c['prefix']}'" if c.get("prefix") else "")
               + (f" --l1-align {c['l1a']}" if c.get("l1a") else "")
               + "".join(f" --backend-param {kv}" for kv in c.get("bp", [])))
        with open(f"{out}.log", "w") as lf:
            subprocess.run(["bash", "-c", cmd], stdout=lf, stderr=subprocess.STDOUT, check=True)
        if json.loads((out / "summary.json").read_text()).get("storage_restart"):
            S.log("  !! 실행 중 storage 가 다시 떴다 — 이 실행은 무효. hot-update 가 풀렸을 수 있어 지금 설정을 다시 건다")
            S.apply(dict(S.CUR), force=True)
        return out

    def measure(self, tag, c, d, level):
        m = A.metrics(self.run(tag, c, d, level))
        keep = ("ok", "valid", "speed", "late_p99", "store_ratio", "missed_frac", "load_s_p99", "util_disk_read", "util_disk_write")
        self.res.setdefault(tag, {})[f"{level:.2f}"] = {k: m.get(k) for k in keep}
        self.res_path.write_text(json.dumps(self.res, ensure_ascii=False, indent=1))
        print(f"{tag:22} " + A.line(m), flush=True)
        return m

    def search(self, tag, c, d, start):
        """start 에서 따라가면 한 단계씩 올리고(2번 이어 실패 · 크게 실패면 멈춤), 못 따라가면 따라갈 때까지 내린다. 최대 단계."""
        i, res = GRID.index(start), {}
        m = res[GRID[i]] = self.measure(tag, c, d, GRID[i])
        if m["ok"]:
            bad = 0
            while i + 1 < len(GRID):
                i += 1
                m = res[GRID[i]] = self.measure(tag, c, d, GRID[i])
                bad = 0 if m["ok"] else bad + 1
                if bad >= 2 or (not m["ok"] and ((m["late_p99"] or 0) > 5 or (m["store_ratio"] or 0) < 0.8)):
                    break
        else:
            while i > 0 and not m["ok"]:
                i -= 1
                m = res[GRID[i]] = self.measure(tag, c, d, GRID[i])
        best = max((lv for lv, x in res.items() if x["ok"]), default=None)
        S.log(f"   {tag} 최대 " + (f"{best:.2f} (x{res[best]['speed']:.2f})" if best else f"{GRID[0]:.2f} 미만"))
        return best

    def repeat(self, knobs, level, times, first=2, settings=("base",)):
        """search 대신 그 단계만 다시 잰다(tag 끝 .r2 · .r3 …) — 한 단계 차이를 세 번 중 두 번으로 정할 때"""
        for name in settings:
            f = SETTINGS[name]
            try:
                S.apply({**S.BASE, **{k: v for k, v in f.items() if k != "chunk"}})
            except S.NotAllowed as e:
                S.log(f"   {name}: {e} 을 바꿀 허락이 없어 건너뛴다")
                continue
            for k in knobs:
                d = data_dir(self.ver, k, f.get("chunk"), self.wm)
                oks = [self.measure(f"{k}+{name}.r{i}", self.v["knobs"][k], d, level)["ok"] for i in range(first, first + times)]
                S.log(f"   {k}+{name} {level:.2f} 다시 {times}번: 통과 {sum(oks)}")

    def go(self, knobs, settings, start, set_start=None):
        best_knob, best_lv = None, None
        S.apply({**S.BASE})
        for k in knobs:                                          # ① 클러스터 기본값에서 adapter 설정마다
            lv = self.search(f"{k}+base", self.v["knobs"][k], data_dir(self.ver, k, None, self.wm), start)
            if lv and (best_lv is None or lv > best_lv):
                best_knob, best_lv = k, lv
        if best_knob is None:
            S.log("어느 adapter 설정도 가장 낮은 단계를 따라가지 못했다")
            return
        S.log(f"가장 좋은 adapter 설정: {best_knob}")
        for name in settings:                                    # ② 그 설정에서 클러스터 설정마다
            if name == "base":
                continue
            f = SETTINGS[name]
            try:
                S.apply({**S.BASE, **{k: v for k, v in f.items() if k != "chunk"}})
            except S.NotAllowed as e:
                S.log(f"   {name}: {e} 을 바꿀 허락이 없어 건너뛴다")
                continue
            self.search(f"{best_knob}+{name}", self.v["knobs"][best_knob], data_dir(self.ver, best_knob, f.get("chunk"), self.wm), set_start or best_lv)


# --- verify ---
def compare_io(run, rules):
    """compare.py 의 라벨을 3FS 데이터 IO(load · store · dup · 없음)로 묶어 줄인 trace 의 부분마다 견준다."""
    from lmcache.v1.multiprocess.token_hasher import TokenHasher
    from cache_sim import CacheSim
    from compare import gpu_blocks, real_labels_open
    from send import prompt, requests
    io = {"L1": "없음", "lost": "없음", "L2": "load", "store": "store", "dup": "dup"}
    cfg = json.loads((SM / "targets/serving-base.json").read_text())
    cfg["lmcache"]["rules"] = rules
    meta = json.loads((run / "requests.json").read_text())
    rq = meta["requests"]
    reqs, block = requests(meta["trace"], meta["minutes"], meta.get("max_requests"), meta.get("max_len"))
    reqs = reqs[:len(rq)]
    th = TokenHasher(cfg["lmcache"]["chunk_size"], cfg["lmcache"]["hash_algorithm"])
    toks = [prompt(r, block) for r in reqs]
    cks = [[h.hex()[:16] for h in th.compute_chunk_hashes(t)] for t in toks]
    real = real_labels_open(run, cks, rq, cfg["lmcache"]["chunk_size"])
    sims = {n: CacheSim(cfg, gpu_blocks(run, cfg["vllm"]["block_size"])) for n in meta["nodes"]}
    l2, sub = set(), set()
    for s in sims.values():
        s.l2, s.submitted = l2, sub
    simres = [sims[r["node"]].request(t, int(r["max_tokens"]), ck) for t, ck, r in zip(toks, cks, rq)]
    parts = collections.defaultdict(lambda: {"n": 0, "agree": 0, "real": collections.Counter(), "sim": collections.Counter()})
    for r, a, s in zip(reqs, real, simres):
        p = parts[r.get("part", "all")]
        for x, y in zip(a, s["labels"]):
            p["n"] += 1
            p["agree"] += io[x] == io[y]
            p["real"][io[x]] += 1
            p["sim"][io[y]] += 1
    evs = [json.loads(l) for l in open(run / "adapter.jsonl")]
    return {"parts": {k: {"chunks": v["n"], "io_agree": v["agree"] / v["n"], "real": dict(v["real"]), "sim": dict(v["sim"])}
                      for k, v in parts.items()},
            "lookup_keys": {"real": sum(len(e["keys"]) for e in evs if e["op"] == "lookup"),
                            "sim": sum(len(c) - s["lookup_from"] for c, s in zip(cks, simres) if s["lookup_from"] < len(c))}}


# 10-20 · 10-21 대조 셋: 창 load 는 시뮬레이터가 늘 4-5% 많다(비우기 시점의 치우침 — dyn +4.3% · a +5.3%), store · lookup 은 정확히 맞는다
PASS = dict(io_agree=0.95, load=(-0.05, 0.07), store=0.02, lookup=0.02)


def verify(ver):
    v = VERSIONS[ver]
    if not v["real"]:
        raise SystemExit(f"{ver} 는 실제 대조용 서버 구성이 없다(validate/node_up.sh 의 ADAPTER)")
    name = f"hot30-{ver}" + ("-mpdef" if os.environ.get("MP_FLAGS") == "lmcache-default" else "")   # MP 서버 flag 를 LMCache 기본으로(launch_mp.py)
    l2 = f"/3fs/3fs/pty/sim-mp/{name}"
    if os.path.exists(l2):
        raise SystemExit(f"{l2} 가 이미 있다. 앞 실행의 L2 가 섞이면 대조가 깨진다")
    why = busy()
    if why:
        raise SystemExit("다른 작업이 보여 멈춘다: " + "; ".join(why))
    env = dict(os.environ, NODES=" ".join(c["host"] for c in C.CLIENTS), MAX_SEQS="default", SPEED="0.01", MAX_TOKENS="trace",
               OUTDIR=f"tune/{ver}", ADAPTER=v["real"])
    try:
        subprocess.run(["./run_real.sh", name, "alibaba-coder-hot30", "1000"], cwd=SM / "validate", env=env, check=True)
    finally:
        shutil.rmtree(l2, ignore_errors=True)               # 대조용 L2 는 이 실행만 쓴다
    run = OUT / ver / name
    r = compare_io(run, v["rules"])
    w = r["parts"]["window"]
    err = {k: (w["sim"].get(k, 0) - w["real"].get(k, 0)) / max(1, w["real"].get(k, 0)) for k in ("load", "store")}
    lk = (r["lookup_keys"]["sim"] - r["lookup_keys"]["real"]) / max(1, r["lookup_keys"]["real"])
    r["verdict"] = {"io_agree": w["io_agree"] >= PASS["io_agree"], "load": PASS["load"][0] <= err["load"] <= PASS["load"][1],
                    "store": abs(err["store"]) <= PASS["store"], "lookup": abs(lk) <= PASS["lookup"]}
    (run / "verify.json").write_text(json.dumps(r, ensure_ascii=False, indent=1))
    print(f"{ver} 창 3FS IO 일치 {w['io_agree']:.2%} · load {err['load']:+.1%} · store {err['store']:+.1%} · lookup key {lk:+.1%} → "
          + ("같다" if all(r["verdict"].values()) else "어긋남: " + ", ".join(k for k, ok in r["verdict"].items() if not ok)
             + " — 시뮬레이터 규칙(cache_sim.py lmcache.rules)을 이 버전에 맞춘다"))


def table():
    for d in sorted(OUT.glob("*/result.json")):
        res = json.loads(d.read_text())
        print(f"== {d.parent.name} ({VERSIONS.get(d.parent.name, {}).get('name', '')})")
        for tag, lv in res.items():
            ok = [float(k) for k, m in lv.items() if m["ok"]]
            b = max(ok) if ok else None
            print(f"  {tag:22} " + (f"x{lv[f'{b:.2f}']['speed']:.2f}" if b else f"단계 {GRID[0]:.2f} 미만"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["verify", "sweep", "table"])
    ap.add_argument("ver", nargs="?", choices=list(VERSIONS))
    ap.add_argument("--knobs", help="adapter 설정 (기본: 그 버전 전부)")
    ap.add_argument("--settings", default="base,b1m,r32-1m,r6,rts,c512k", help="클러스터 설정")
    ap.add_argument("--start", type=float, help="처음 잴 단계 (기본: 버전의 start)")
    ap.add_argument("--trace", default="coder", choices=list(WORKLOADS), help="trace 구간")
    ap.add_argument("--model", default="qwen3-14b", choices=list(MODELS), help="모델(token 당 KV 크기)")
    ap.add_argument("--repeat", type=int, default=0, help="search 대신 --start 단계만 이 횟수만큼 다시 잰다(실험 기준 REF 에서)")
    ap.add_argument("--rep-start", type=int, default=2, help="--repeat 의 tag 첫 번호(.rN) — 앞서 쓴 번호와 겹치지 않게")
    ap.add_argument("--set-start", type=float, help="클러스터 설정 search 의 첫 단계(기본: adapter 설정의 최대 단계)")
    ap.add_argument("--evict", help="L1 비우기 문턱/비율(LMCache 기본 0.8/0.2). plan 을 시뮬레이터로 다시 낸다 — IO 양이 바뀌니 단계가 아니라 배속으로 견준다")
    args = ap.parse_args()
    if args.cmd == "table":
        return table()
    if not args.ver:
        raise SystemExit("버전을 준다: " + ", ".join(VERSIONS))
    if args.cmd == "verify":
        return verify(args.ver)
    bad = S.check_home()
    if bad:
        raise SystemExit("클러스터가 되돌릴 곳에 있지 않다 — 되돌리면 지금 값을 지운다. 확인하고 맞춘 뒤 다시: " + "; ".join(bad))
    for s in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, lambda *_: sys.exit(1))
    sw = Sweep(args.ver, args.trace, args.model, args.evict)
    knobs = args.knobs.split(",") if args.knobs else list(sw.v["knobs"])
    try:
        if args.repeat:
            sw.repeat(knobs, args.start, args.repeat, args.rep_start, args.settings.split(",") if args.settings != ap.get_default("settings") else ("base",))
        else:
            sw.go(knobs, args.settings.split(","), args.start or sw.v["start"], args.set_start)
    finally:
        S.restore()
        shutil.rmtree(DATA, ignore_errors=True)
        S.log(f"재생 폴더 지움: {not os.path.exists(DATA)}")


if __name__ == "__main__":
    main()
