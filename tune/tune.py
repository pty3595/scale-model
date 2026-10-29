#!/usr/bin/env python3
"""설정 sweep 도구 — 그때의 클러스터 상태에서 L2 adapter 종류마다 소프트웨어 설정의 최적값을 같은 replay 로 찾고,
replay 가 그 adapter 의 실제 IO 를 아직 내는지 chunk 대조로 확인한다. LMCache 가 바뀌면 verify → (어긋나면 시뮬레이터 규칙 고침) → sweep.

  tune.py verify ADAPTER                 실제 vLLM + 계측 LMCache 3대 대 시뮬레이터, 데워진 구간 chunk 대조 (약 1.5시간)
  tune.py sweep ADAPTER [--knobs a,b] [--settings x,y] [--trace T] [--model M] [--clients N]
                                         adapter 설정마다 → 가장 좋은 것에 FUSE · layout 설정마다, 따라가는 최대 배속
                                         (trace 구간 · 모델의 KV 크기를 바꿔 같은 sweep — WORKLOADS · MODELS)
  tune.py table                          결과 표

ADAPTER 는 nixl_store_dynamic · nixl_native · hf3fs · raw_hf3fs(PR 5828, 클라이언트 1대) — 종류마다 하나(ADAPTERS). 클러스터 설정 목록은 SETTINGS. 실행마다 다른 사람의 작업이 없는지 보고, 끝나면(어떻게 끝나든) FUSE · storage 를 되돌린다.
클러스터 고유 값과 허락 범위는 clusters/<이름>.toml(cluster.py). 허락이 없는 쪽을 바꾸는 설정은 건너뛴다. 재시작이 필요한 설정은 SETTINGS 에 없다.
시작 때 클러스터가 되돌릴 곳(템플릿 사본 · [home])에 있는지 보고, 아니면 멈춘다.
replay 의 IO 는 LMCache 규칙(cache_sim.py 의 lmcache.rules)으로 만든 plan 을 쓴다.
LMCache 는 [[client]] python 의 것을 쓴다 — RULES 의 commit(README "기준 버전") 하나에 adapter 셋이 다 있다.
adapter 마다 빌드를 따로 두었으면 clusters/<이름>.toml [builds.<adapter>] 의 python · pythonpath (클라이언트마다 같은 경로).
판정은 analyze.py, plan 묶음은 families.py, 클러스터 층 설정은 settings.py(results/ 의 10-12 · 10-18 코드에서 옮겼다).
"""
import argparse
import collections
import json
import math
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

RULES = "ad1d738"                    # 시뮬레이터 규칙 = 맞춘 LMCache commit (cache_sim.py 머리말, README "기준 버전")
OUT = SM / f"results/tune-{RULES}"   # 규칙마다 따로. 8d7d2c4 · 배속 격자 결과는 results/tune-speed, 그 전 단계 격자는 results/tune
DATA = C.data("tune")
GRID = [float(f"{2 ** (k / 5):.3g}") for k in range(-10, 31)]   # 배속 x0.25-x64. 한 칸 15%(2^(1/5)), 다섯 칸이 두 배
JUMP, START = 5, 1.0                 # search 는 START 에서 두 배씩 건너 경계를 찾은 뒤 한 칸씩 올린다
MEM = "g24-l32"
MIN_FREE = 0.15                      # 3FS 여유가 이보다 적으면 실행을 시작하지 않는다
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


RAW = {k: C.C["threefs"].get(k, "") for k in ("mgmtd", "cluster_id", "ib_devices")}   # raw_hf3fs 가 FUSE 없이 붙을 곳 — FUSE launcher 설정의 값


def raw(nw, ops=None, conc=None):
    """raw_hf3fs(PR 5828) 설정. 파일은 재생 폴더 하나(base_dir = 3FS 루트 기준 {rdir}).
    conc = (max_concurrent_requests, _per_server, max_batch_bytes) — 없으면 PR 기본 256 · 64 · 32 MiB. 읽기 · 쓰기가 같은 값을 쓴다"""
    c = dict(zip(("max_concurrent_requests", "max_concurrent_requests_per_server", "max_batch_bytes"), conc or ()))
    return json.dumps({"type": "raw_hf3fs", "num_workers": nw, "meta_addr": RAW["mgmtd"], "mgmtd_addr": RAW["mgmtd"],
                       "cluster_id": RAW["cluster_id"], "base_dir": "{rdir}", **({"per_op_workers": ops} if ops else {}), **c},
                      separators=(",", ":"))


def build(adapter):
    """그 adapter 를 돌릴 LMCache — [builds.<adapter>] 의 python · pythonpath. 없으면 [[client]] python, PYTHONPATH 없음"""
    b = C.C.get("builds", {}).get(adapter, {})
    return b.get("python"), (f"PYTHONPATH={b['pythonpath']}" if b.get("pythonpath") else "")


# NIXL dram_zc 의 2 GiB 버그 shim(LD_PRELOAD). 클라이언트마다 이 경로에 있어야 한다 — 없으면 그 knob 을 돌리기 전에 멈춘다
PW_SHIM = C.C.get("builds", {}).get("pwrite_shim", "/tmp/pwrite_full.so")
# adapter 종류마다 하나: 시뮬레이터의 LMCache 규칙, 실제 대조(verify)를 하는가, adapter 설정(knob)마다 replay 인자
ADAPTERS = {
    "nixl_store_dynamic": dict(rules=RULES, verify=True, knobs={
        "dram": dict(nd=1),                                                   # calibration 의 adapter (문서 예시 아님 — 문서는 mem_config 를 안 준다)
        "auto": dict(nd=1, bp=["mem_config=auto"]),                           # nixl.rst 예시 = mem_config 없음 = plugin 기본값 auto
        "dram_zc": dict(nd=1, bp=["mem_config=dram_zc"], prefix=f"LD_PRELOAD={PW_SHIM}"),   # NIXL 2 GiB 버그 shim
        "dram_zc-a4k": dict(nd=1, bp=["mem_config=dram_zc"], prefix=f"LD_PRELOAD={PW_SHIM}", l1a=4096),   # L1 정렬 LMCache 기본(4 KiB)
        "dram_zc-a2m": dict(nd=1, bp=["mem_config=dram_zc"], prefix=f"LD_PRELOAD={PW_SHIM}", l1a=2097152),
        "dram_zc-nodio": dict(nd=1, bp=["mem_config=dram_zc", "use_direct_io=false"], prefix=f"LD_PRELOAD={PW_SHIM}")}),   # O_DIRECT 끔   # 2 MiB — 8 MiB 가 나은 까닭이 huge page 경계인가
    "nixl_native": dict(rules=RULES, verify=False, knobs={
        "w4": dict(nd=1, aj=native(4)),
        "w16": dict(nd=1, aj=native(16)),
        "w16-a4k": dict(nd=1, aj=native(16), l1a=4096),   # L1 정렬 LMCache 기본(4 KiB)
        # 10-22: nixl_store_dynamic 은 dram → dram_zc 가 0.07 → 0.20 이었다. native 는 dram 으로만 쟀다
        "w16-zc": dict(nd=1, aj=native(16, "dram_zc"), prefix=f"LD_PRELOAD={PW_SHIM}"),
        "w16-zc-a4k": dict(nd=1, aj=native(16, "dram_zc"), prefix=f"LD_PRELOAD={PW_SHIM}", l1a=4096),
        "w16-nodio": dict(nd=1, aj=native(16, dio="false"))}),   # O_DIRECT 끔
    "hf3fs": dict(rules=RULES, verify=True, knobs={
        "w4-d1": dict(nd=1, aj=hf3fs(1, False)),
        "s16-d1": dict(nd=1, aj=hf3fs(1, True)),
        "s16-d16": dict(nd=16, aj=hf3fs(16, True)),
        "s16-d16-a4k": dict(nd=16, aj=hf3fs(16, True), l1a=4096),   # L1 정렬 LMCache 기본(4 KiB)
        # LMCache prefetch 동시 수(--l2-prefetch-max-in-flight, 기본 8) = replay --slots. 같은 폴더를 함께 쓴다(data_dir 는 knob 이름마다)
        "s16-d16-sl4": dict(nd=16, aj=hf3fs(16, True), slots=4),
        "s16-d16-sl16": dict(nd=16, aj=hf3fs(16, True), slots=16),
        "s16-d1-sl4": dict(nd=1, aj=hf3fs(1, True), slots=4),
        "s16-d1-sl16": dict(nd=1, aj=hf3fs(1, True), slots=16),
        "doc": dict(nd=2, aj=hf3fs_doc())}),             # hf3fs.rst 예시
    # PR 5828(10-25, 열림) — FUSE · usrbio 없이 3FS storage client 로 직접, 파일 하나에 offset. 빌드는 HANDOFF.
    # key → offset 표가 프로세스 메모리에만 있어 다른 클라이언트가 쓴 chunk 를 못 찾는다 — 클라이언트 1대 plan 으로만(clients).
    # chunk 를 모든 chain 에 직접 쓴다 — 재생 폴더가 stripe = chain 수 · chunk = connector 의 chunk_size(4 MiB)여야(layout)
    # FUSE 로 파일을 지울 때 meta 가 모든 chain 에서 지운다(meta dynamic_stripe 가 꺼져 있어야 — 켜져 있으면 일부 chain 만)
    "raw_hf3fs": dict(rules=RULES, verify=False, clients=1, layout=4194304, knobs={
        "w4": dict(nd=1, aj=raw(4)),                                                       # PR 의 l2 bench 설정
        "doc": dict(nd=1, aj=raw(4, {"lookup": 2, "retrieve": 16, "store": 4})),           # raw_hf3fs.rst 예시
        "s16": dict(nd=1, aj=raw(4, {"lookup": 2, "retrieve": 16, "store": 16})),          # hf3fs s16 과 같은 lane
        # 10-28 w4(PR 기본 256 · 64 · 32 MiB)는 x0.5 에서도 load + store 약 1.5 GiB/s 에 막히고 storage 수신 버림이 hf3fs 의 몇 배 —
        # 손실 망에서 한꺼번에 돌아오는 바이트(동시 수 x 묶음)가 많다(HANDOFF "망 손실과 읽기 설정": 24-32 MB 이하면 깨끗). connector chunk 가 4 MiB 라 묶음도 4 MiB
        "c8-2": dict(nd=1, aj=raw(4, conc=(8, 2, 4 << 20))),                                # 32 MB — 손실 없는 한도
        "c12-3": dict(nd=1, aj=raw(4, conc=(12, 3, 4 << 20))),                              # FUSE 기준 읽기(12/3 · 4MB)
        "c64-32": dict(nd=1, aj=raw(4, conc=(64, 32, 4 << 20)))}),                          # FUSE 기준 쓰기(64/32 · 4MB)
}
for k in ADAPTERS["raw_hf3fs"]["knobs"].values():
    k["prefix"] = f"LMCACHE_3FS_IB_DEVICES={RAW['ib_devices']}" if RAW["ib_devices"] else ""   # FUSE 와 같은 RDMA 장치(로컬 패치)
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
def family(rules, wl="coder", model="qwen3-14b", ev=None, n=None):
    """trace 구간 하나 x 메모리 설정 x LMCache 규칙 x 모델(x L1 비우기 문턱 · 비율 x 클라이언트 수). a44d5e5 · 기본 모델은 예전 sweep 의 plan 그대로.
    n: 클라이언트(= LMCache 서버) 수, 기본 클러스터의 [[client]] 전부. 적으면 같은 trace 구간을 그 대수가 나눠 받는다"""
    trace, start, minutes = WORKLOADS[wl]
    n = n or len(C.CLIENTS)
    if rules == "a44d5e5" and model == "qwen3-14b" and not ev and n == len(C.CLIENTS):
        return F.make(trace, start, minutes, MEM)
    d = SM / "plans/tune" / (f"{F.fam_name(trace, start, minutes, MEM)}-{rules}" + ("" if model == "qwen3-14b" else f"-{model}")
                             + (f"-ev{ev.replace('/', '-')}" if ev else "") + ("" if n == len(C.CLIENTS) else f"-n{n}"))
    if not (d / "base.json").exists():
        cfg = F.serving_cfg(MEM)
        cfg["lmcache"]["rules"] = rules
        cfg["model"]["kv_bytes_per_token"] = MODELS[model]
        if ev:                                             # LMCache --eviction-trigger-watermark / --eviction-ratio (기본 0.8 / 0.2)
            w, r = ev.split("/")
            cfg["lmcache"]["eviction_trigger_watermark"], cfg["lmcache"]["eviction_ratio"] = float(w), float(r)
        d.mkdir(parents=True, exist_ok=True)
        (d / "base.json").write_text(json.dumps(P.simulate(trace, start, minutes, n, cfg)))
    return d


# --- 재생 폴더 ---
STRIPE, CHUNK = C.C["measured"]["default_stripe"], C.C["measured"]["chunk_bytes"]   # 그 클러스터의 3FS 기본 layout


def shim_ok(prefix, _seen={}):
    """prefix 가 LD_PRELOAD 를 쓰면 그 파일이 모든 클라이언트에 있는지 본다(파일마다 한 번)"""
    for w in prefix.split():
        if w.startswith("LD_PRELOAD="):
            f = w.split("=", 1)[1]
            if f not in _seen:
                _seen[f] = [c["host"] for c in C.CLIENTS if C.sh(C.addr(c["host"]), f"test -f {f} && echo y", check=False).strip() != "y"]
            if _seen[f]:
                raise SystemExit(f"{f} 가 없다: {', '.join(_seen[f])} — 그 파일을 두거나 clusters/<이름>.toml [builds] pwrite_shim 을 고친다")


def on_grid(x):
    """GRID 에서 x 에 가장 가까운 배속"""
    return min(GRID, key=lambda g: abs(math.log(g / x)))


def data_dir(adapter, knob, chunk, wm=""):
    """adapter · knob · chunk 크기(· trace · 모델)마다 폴더 하나. 미리 쓴 chunk 를 배속 · 클러스터 설정이 바뀌어도 다시 쓴다(--keep-prepop)."""
    nd = ADAPTERS[adapter]["knobs"][knob]["nd"]
    d = f"{DATA}/{adapter}{wm}-{knob}" + ("" if chunk is None else f"-c{chunk >> 10}k")
    for x in [d] + ([f"{d}/p{i}" for i in range(nd)] if nd > 1 else []):
        if chunk is None:
            os.makedirs(x, exist_ok=True)
            os.chmod(x, 0o777)
        else:
            ensure_layout(x, STRIPE, chunk)        # 하위 폴더에도 layout 을 직접 건다
    lay = {l.split()[0]: l.split()[-1] for l in admin(f"stat --display-layout {C.rel(d)}").splitlines()
           if l.startswith("Layout-")}
    if lay.get("Layout-ChunkSize") != str(chunk or CHUNK):
        raise SystemExit(f"{d} chunk {lay.get('Layout-ChunkSize')} (원한 것 {chunk or CHUNK})")
    return d


# --- sweep ---
class Sweep:
    def __init__(self, adapter, wl="coder", model="qwen3-14b", ev=None, n=None):
        self.adapter, self.v = adapter, ADAPTERS[adapter]
        n = n or self.v.get("clients") or len(C.CLIENTS)
        if self.v.get("clients") and n != self.v["clients"]:
            raise SystemExit(f"{adapter} 는 클라이언트 {self.v['clients']}대로만 돈다(--clients {self.v['clients']}) — key 표가 프로세스 안에만 있다")
        if adapter == "raw_hf3fs":
            if not (RAW["mgmtd"] and RAW["cluster_id"]):
                raise SystemExit("clusters/<이름>.toml [threefs] 에 mgmtd · cluster_id 가 없다(FUSE launcher 설정의 mgmtd_server_addresses · cluster_id)")
            chains = len(admin("list-chains").splitlines()) - 1
            if chains != STRIPE:
                raise SystemExit(f"chain {chains}개 ≠ 기본 stripe {STRIPE} — raw_hf3fs 의 chunk 가 재생 폴더의 layout 밖 chain 에 남는다")
        self.fam = family(self.v["rules"], wl, model, ev, n)
        self.wm = (("" if wl == "coder" else f"-{wl}") + ("" if model == "qwen3-14b" else f"-{model}") + (f"-ev{ev.replace('/', '-')}" if ev else "")
                   + ("" if n == len(C.CLIENTS) else f"-n{n}"))
        self.out = OUT / f"{adapter}{self.wm}"
        (self.out / "runs").mkdir(parents=True, exist_ok=True)
        self.res_path = self.out / "result.json"
        self.res = json.loads(self.res_path.read_text()) if self.res_path.exists() else {}
        b = json.loads((self.fam / "base.json").read_text())
        S.log(f"plan {self.fam.name}: store÷load {b['store_chunks'] / max(1, b['load_chunks']):.2f}"
              " — 1 을 넘으면 store 벽에 먼저 걸리기 쉬워 store 쪽 설정부터 본다(10-24 coder 60분)")

    def run(self, tag, c, d, speed):
        out = self.out / "runs" / f"{tag}-x{speed:g}"
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
        py, pypath = build(self.adapter)
        prefix = " ".join(x for x in (c.get("prefix", ""), pypath) if x)
        shim_ok(prefix)
        plan = F.speed_plan(self.fam, "full", speed)
        cmd = (f"cd {SM} && exec python3 replay.py {plan} --out {out} --procs 1 --slots {c.get('slots', 8)} --data-dir {d} --keep-prepop"
               + (f" --adapter-json '{c['aj']}'" if c.get("aj") else "")
               + (f" --python '{py}'" if py else "")
               + (f" --prefix '{prefix}'" if prefix else "")
               + (f" --l1-align {c['l1a']}" if c.get("l1a") else "")
               + "".join(f" --backend-param {kv}" for kv in c.get("bp", [])))
        with open(f"{out}.log", "w") as lf:
            subprocess.run(["bash", "-c", cmd], stdout=lf, stderr=subprocess.STDOUT, check=True)
        if json.loads((out / "summary.json").read_text()).get("storage_restart"):
            S.log("  !! 실행 중 storage 가 다시 떴다 — 이 실행은 무효. hot-update 가 풀렸을 수 있어 지금 설정을 다시 건다")
            S.apply(dict(S.CUR), force=True)
        return out

    def measure(self, tag, c, d, speed):
        m = A.metrics(self.run(tag, c, d, speed))
        keep = ("ok", "valid", "speed", "late_p99", "store_ratio", "missed_frac", "load_s_p99", "load", "store", "disk_read", "disk_write",
                "util_disk_read", "util_disk_write", "of_3fs_load", "of_3fs_store")
        self.res.setdefault(tag, {})[f"{speed:g}"] = {k: m.get(k) for k in keep}
        self.res_path.write_text(json.dumps(self.res, ensure_ascii=False, indent=1))
        print(f"{tag:22} " + A.line(m), flush=True)
        return m

    def search(self, tag, c, d, start):
        """start 에서 두 배(JUMP 칸)씩 올려(못 따라가면 내려) 따라가는 칸을 찾고, 거기서 한 칸씩 올린다
        (2번 이어 실패 · 크게 실패면 멈춤). 따라간 가장 높은 배속. 배속마다 trace 의 IO 양이 달라 시작점을 미리 알 수 없다"""
        res = {}

        def at(i):
            if GRID[i] not in res:
                res[GRID[i]] = self.measure(tag, c, d, GRID[i])
            return res[GRID[i]]

        i = GRID.index(start)
        if at(i)["ok"]:
            while i + JUMP < len(GRID) and at(i + JUMP)["ok"]:
                i += JUMP
        else:
            while i > 0 and not at(i)["ok"]:
                i = max(0, i - JUMP)
        if at(i)["ok"]:
            bad = 0
            while i + 1 < len(GRID):
                i += 1
                m = at(i)
                bad = 0 if m["ok"] else bad + 1
                if bad >= 2 or (not m["ok"] and ((m["late_p99"] or 0) > 5 or (m["store_ratio"] or 0) < 0.8)):
                    break
        best = max((sp for sp, x in res.items() if x["ok"]), default=None)
        S.log(f"   {tag} 최대 " + (f"x{best:g} (디스크 읽기 {res[best].get('disk_read') or 0:.2f} GiB/s)" if best else f"x{GRID[0]:g} 미만"))
        return best

    def repeat(self, knobs, speed, times, first=2, settings=("base",)):
        """search 대신 그 배속만 다시 잰다(tag 끝 .r2 · .r3 …) — 한 칸 차이를 세 번 중 두 번으로 정할 때"""
        for name in settings:
            f = SETTINGS[name]
            if self.v.get("layout") and f.get("chunk"):
                S.log(f"   {name}: {self.adapter} 는 재생 폴더 chunk 가 {self.v['layout']} 로 정해져 있어 건너뛴다")
                continue
            try:
                S.apply({**S.BASE, **{k: v for k, v in f.items() if k != "chunk"}})
            except S.NotAllowed as e:
                S.log(f"   {name}: {e} 을 바꿀 허락이 없어 건너뛴다")
                continue
            for k in knobs:
                d = data_dir(self.adapter, k, f.get("chunk") or self.v.get("layout"), self.wm)
                oks = [self.measure(f"{k}+{name}.r{i}", self.v["knobs"][k], d, speed)["ok"] for i in range(first, first + times)]
                S.log(f"   {k}+{name} x{speed:g} 다시 {times}번: 통과 {sum(oks)}")

    def go(self, knobs, settings, start, set_start=None):
        best_knob, best_sp = None, None
        S.apply({**S.BASE})
        for k in knobs:                                          # ① 클러스터 기본값에서 adapter 설정마다
            sp = self.search(f"{k}+base", self.v["knobs"][k], data_dir(self.adapter, k, self.v.get("layout"), self.wm), start)
            if sp and (best_sp is None or sp > best_sp):
                best_knob, best_sp = k, sp
        if best_knob is None:
            S.log(f"어느 adapter 설정도 가장 낮은 배속 x{GRID[0]:g} 을 따라가지 못했다")
            return
        S.log(f"가장 좋은 adapter 설정: {best_knob}")
        for name in settings:                                    # ② 그 설정에서 클러스터 설정마다
            if name == "base":
                continue
            f = SETTINGS[name]
            if self.v.get("layout") and f.get("chunk"):
                S.log(f"   {name}: {self.adapter} 는 재생 폴더 chunk 가 {self.v['layout']} 로 정해져 있어 건너뛴다")
                continue
            try:
                S.apply({**S.BASE, **{k: v for k, v in f.items() if k != "chunk"}})
            except S.NotAllowed as e:
                S.log(f"   {name}: {e} 을 바꿀 허락이 없어 건너뛴다")
                continue
            self.search(f"{best_knob}+{name}", self.v["knobs"][best_knob], data_dir(self.adapter, best_knob, f.get("chunk") or self.v.get("layout"), self.wm), set_start or best_sp)


# --- verify ---
def compare_io(run, rules):
    """compare.py 의 라벨을 3FS 데이터 IO(load · store · dup · 없음)로 묶어 줄인 trace 의 부분마다 견준다."""
    from lmcache.v1.multiprocess.token_hasher import TokenHasher
    from cache_sim import CacheSim
    from compare import gpu_blocks, real_labels_open
    from send import prompt, requests
    io = {"L1": "없음", "lost": "없음", "gpu": "없음", "L2": "load", "store": "store", "dup": "dup"}
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


def verify(adapter):
    v = ADAPTERS[adapter]
    if not v["verify"]:
        raise SystemExit(f"{adapter} 는 실제 대조용 서버 구성이 없다(validate/node_up.sh 의 ADAPTER)")
    name = f"hot30-{adapter}" + ("-mpdef" if os.environ.get("MP_FLAGS") == "lmcache-default" else "")   # MP 서버 flag 를 LMCache 기본으로(launch_mp.py)
    l2 = f"/3fs/3fs/pty/sim-mp/{name}"
    if os.path.exists(l2):
        raise SystemExit(f"{l2} 가 이미 있다. 앞 실행의 L2 가 섞이면 대조가 깨진다")
    why = busy()
    if why:
        raise SystemExit("다른 작업이 보여 멈춘다: " + "; ".join(why))
    env = dict(os.environ, NODES=" ".join(c["host"] for c in C.CLIENTS), MAX_SEQS="default", SPEED="0.01", MAX_TOKENS="trace",
               OUTDIR=f"{OUT.relative_to(SM / 'results')}/{adapter}", ADAPTER=adapter)   # 아래 run 과 같은 폴더
    try:
        subprocess.run(["./run_real.sh", name, "alibaba-coder-hot30", "1000"], cwd=SM / "validate", env=env, check=True)
    finally:
        shutil.rmtree(l2, ignore_errors=True)               # 대조용 L2 는 이 실행만 쓴다
    run = OUT / adapter / name
    r = compare_io(run, v["rules"])
    w = r["parts"]["window"]
    err = {k: (w["sim"].get(k, 0) - w["real"].get(k, 0)) / max(1, w["real"].get(k, 0)) for k in ("load", "store")}
    lk = (r["lookup_keys"]["sim"] - r["lookup_keys"]["real"]) / max(1, r["lookup_keys"]["real"])
    r["verdict"] = {"io_agree": w["io_agree"] >= PASS["io_agree"], "load": PASS["load"][0] <= err["load"] <= PASS["load"][1],
                    "store": abs(err["store"]) <= PASS["store"], "lookup": abs(lk) <= PASS["lookup"]}
    (run / "verify.json").write_text(json.dumps(r, ensure_ascii=False, indent=1))
    print(f"{adapter} 창 3FS IO 일치 {w['io_agree']:.2%} · load {err['load']:+.1%} · store {err['store']:+.1%} · lookup key {lk:+.1%} → "
          + ("같다" if all(r["verdict"].values()) else "어긋남: " + ", ".join(k for k, ok in r["verdict"].items() if not ok)
             + " — 시뮬레이터 규칙(cache_sim.py lmcache.rules)을 이 LMCache 에 맞춘다"))


def table():
    for d in sorted(OUT.glob("*/result.json")):
        res = json.loads(d.read_text())
        print(f"== {d.parent.name}")
        for tag, sp in res.items():
            ok = [k for k, m in sp.items() if m["ok"]]
            b = sp[max(ok, key=float)] if ok else None
            print(f"  {tag:22} " + (f"x{b['speed']:.3g}" + (f"  디스크 읽기 {b['disk_read']:.2f} GiB/s" if b.get("disk_read") is not None else "")
                                    + (f"  3FS 천장 대비 load {b['of_3fs_load']:.2f}" if b.get("of_3fs_load") is not None else "")
                                    if b else f"x{GRID[0]:g} 미만"))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("cmd", choices=["verify", "sweep", "table"])
    ap.add_argument("adapter", nargs="?", choices=list(ADAPTERS))
    ap.add_argument("--knobs", help="adapter 설정 (기본: 그 adapter 의 knob 전부)")
    ap.add_argument("--settings", default="base,b1m,r32-1m,r6,rts,c512k", help="클러스터 설정")
    ap.add_argument("--start", type=float, help=f"처음 잴 배속 (기본 {START:g}). 격자에서 가장 가까운 칸으로 맞춘다")
    ap.add_argument("--trace", default="coder", choices=list(WORKLOADS), help="trace 구간")
    ap.add_argument("--model", default="qwen3-14b", choices=list(MODELS), help="모델(token 당 KV 크기)")
    ap.add_argument("--repeat", type=int, default=0, help="search 대신 --start 배속만 이 횟수만큼 다시 잰다(실험 기준 REF 에서)")
    ap.add_argument("--rep-start", type=int, default=2, help="--repeat 의 tag 첫 번호(.rN) — 앞서 쓴 번호와 겹치지 않게")
    ap.add_argument("--set-start", type=float, help="클러스터 설정 search 의 첫 배속(기본: adapter 설정의 최대 배속)")
    ap.add_argument("--evict", help="L1 비우기 문턱/비율(LMCache 기본 0.8/0.2). plan 을 시뮬레이터로 다시 낸다(IO 양이 바뀐다)")
    ap.add_argument("--clients", type=int, help="클라이언트 수(기본: 그 adapter 가 정한 수, 없으면 [[client]] 전부). 같은 trace 구간을 그 대수가 나눠 받는다")
    args = ap.parse_args()
    if args.cmd == "table":
        return table()
    if not args.adapter:
        raise SystemExit("adapter 를 준다: " + ", ".join(ADAPTERS))
    if args.cmd == "verify":
        return verify(args.adapter)
    bad = S.check_home()
    if bad:
        raise SystemExit("클러스터가 되돌릴 곳에 있지 않다 — 되돌리면 지금 값을 지운다. 확인하고 맞춘 뒤 다시: " + "; ".join(bad))
    for s in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, lambda *_: sys.exit(1))
    sw = Sweep(args.adapter, args.trace, args.model, args.evict, args.clients)
    knobs = args.knobs.split(",") if args.knobs else list(sw.v["knobs"])
    try:
        start = on_grid(args.start or START)
        if args.repeat:
            sw.repeat(knobs, start, args.repeat, args.rep_start, args.settings.split(",") if args.settings != ap.get_default("settings") else ("base",))
        else:
            sw.go(knobs, args.settings.split(","), start, args.set_start and on_grid(args.set_start))
    finally:
        S.restore()
        shutil.rmtree(DATA, ignore_errors=True)
        S.log(f"재생 폴더 지움: {not os.path.exists(DATA)}")


if __name__ == "__main__":
    main()
