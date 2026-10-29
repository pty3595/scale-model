#!/usr/bin/env python3
"""클라이언트 한 대에서 L2 adapter를 계획된 양만큼 구동한다.

부하를 만드는 코드는 LMCache 의 L2 벤치마크(`lmcache bench l2`)를 그대로 쓴다.
그 하네스에 없는 것은 요청률 제한 하나여서, 여기서는 라운드를 절대 시각표에
맞춰 출발시키는 것만 더한다. `bench_store`/`bench_load` 는 라운드마다
`keys_for_round(r)` 를 먼저 부르고 그 다음에 시간을 재기 시작하므로,
그 안에서 기다리면 측정 구간을 건드리지 않고 양만 줄일 수 있다.
"""

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path

GIB = 1024**3
MIB = 1024**2
MISS_BASE = 10**12      # 미적중 key idx. 데이터셋(수천 개)과 겹치지 않는다
STORE_BASE = 10**11     # 계획대로 도는 store 의 key idx. 라운드마다 새 key 를 쓴다
HERE = Path(__file__).resolve().parent


def load_lmcache():
    """LMCache 의 벤치마크 부품을 그대로 가져온다."""
    from lmcache.cli.commands.bench.l2_adapter_bench.data import (
        create_l1_memory_desc, make_aligned_tensor, make_memory_objects,
        make_object_keys,
    )
    from lmcache.cli.commands.bench.l2_adapter_bench.runner import (
        _PLACEHOLDER_LAYOUT_DESC, _bitmap_count, _wait_lookup_finished,
        bench_load, bench_lookup, bench_store,
    )
    from lmcache.v1.distributed.l2_adapters import create_l2_adapter
    from lmcache.v1.distributed.l2_adapters.config import (
        parse_args_to_l2_adapters_config,
    )
    return dict(
        create_l1_memory_desc=create_l1_memory_desc,
        make_aligned_tensor=make_aligned_tensor,
        make_memory_objects=make_memory_objects,
        make_object_keys=make_object_keys,
        bench_load=bench_load, bench_lookup=bench_lookup, bench_store=bench_store,
        layout_desc=_PLACEHOLDER_LAYOUT_DESC, bitmap_count=_bitmap_count,
        wait_lookup_finished=_wait_lookup_finished,
        create_l2_adapter=create_l2_adapter,
        parse_args_to_l2_adapters_config=parse_args_to_l2_adapters_config,
    )


class Pacer:
    """라운드 r 을 `start + phase + r * period` 에 출발시킨다.

    상대 sleep 이 아니라 절대 시각표를 쓴다. 한 라운드가 예상보다 오래 걸려도
    다음 라운드가 밀리지 않고, 따라잡을 수 없으면 늦은 라운드로 세어 남긴다.
    """

    def __init__(self, period_s: float, phase_s: float = 0.0):
        self.period = period_s
        self.phase = phase_s
        self.start = None
        self.late_rounds = 0
        self.total_wait_s = 0.0

    def wait_for(self, r: int) -> None:
        now = time.perf_counter()
        if self.start is None:
            self.start = now
        due = self.start + self.phase + r * self.period
        gap = due - now
        if gap > 0:
            self.total_wait_s += gap
            time.sleep(gap)
        elif gap < 0:
            self.late_rounds += 1


def build_adapter(lm, adapter_cfg_json: str, l1_memory_desc):
    import argparse as _ap
    ns = _ap.Namespace(l2_adapter=[adapter_cfg_json])
    cfg = lm["parse_args_to_l2_adapters_config"](ns)
    if not cfg.adapters:
        raise SystemExit("adapter 설정을 읽지 못했다")
    return lm["create_l2_adapter"](cfg.adapters[0], l1_memory_desc=l1_memory_desc)


def adapter_json(cal: dict, file_path: str, overrides=None) -> str:
    a = cal["adapter"]
    params = dict(a["backend_params"], file_path=file_path)
    for kv in overrides or []:
        k, _, v = kv.partition("=")
        params[k] = v
    return json.dumps({"type": a["type"], "backend": a["backend"],
                       "backend_params": params})


def apply_env(cal: dict) -> None:
    """HF3FS 백엔드가 뜨는 환경을 맞춘다. 필요하면 자기 자신을 다시 실행한다.

    NIXL 의 HF3FS 플러그인은 cu13 플러그인 디렉터리에만 있고, 그 플러그인의
    RUNPATH 가 빌드 트리를 가리켜 `LD_LIBRARY_PATH` 로 보정해야 열린다.
    동적 로더는 `LD_LIBRARY_PATH` 를 프로세스 시작 시각에 읽으므로
    파이썬 안에서 바꾸면 늦다. 그래서 값을 채운 뒤 `execve` 로 다시 뜬다.
    """
    venv = os.environ.get("VIRTUAL_ENV") or str(Path(sys.executable).parent.parent)
    wanted = {k: v.replace("{venv}", venv) for k, v in cal["adapter"]["env"].items()}
    for k, v in (("LMCACHE_DISABLE_BANNER", "1"), ("DO_NOT_TRACK", "1"),
                 ("LMCACHE_TRACK_USAGE", "false")):
        os.environ.setdefault(k, v)

    os.environ.setdefault("NIXL_PLUGIN_DIR", wanted["NIXL_PLUGIN_DIR"])

    libs = wanted["LD_LIBRARY_PATH"]
    current = os.environ.get("LD_LIBRARY_PATH", "")
    if libs in current.split(":"):
        return
    if os.environ.get("_SCALE_MODEL_REEXEC"):
        raise SystemExit(f"LD_LIBRARY_PATH 를 {libs} 로 맞추지 못했다")
    os.environ["LD_LIBRARY_PATH"] = f"{libs}:{current}" if current else libs
    os.environ["_SCALE_MODEL_REEXEC"] = "1"
    os.execve(sys.executable, [sys.executable] + sys.argv, os.environ)


def run(args) -> dict:
    cal = json.loads(Path(args.calibration).read_text())
    apply_env(cal)
    lm = load_lmcache()

    if args.plan:
        plan = json.loads(Path(args.plan).read_text())
        r = plan["run"]
        op, num_keys, in_flight = r["op"], r["num_keys"], r["in_flight"]
        data_size = r["data_size_kb"] * 1024
        align = r["l1_align_bytes"]
        period = r["round_period_s"]
        want_gibps = plan["ours"]["per_client_gibps"]
        lookup_keys = r.get("lookup_keys")
    else:
        shape = cal["shape"]
        op = args.op
        num_keys = args.num_keys or shape[f"{op}_num_keys"]
        in_flight = args.in_flight
        data_size = (args.data_size_kb or shape["data_size_kb"]) * 1024
        align = shape["l1_align_bytes"]
        period = None          # calibrate / prepare 는 최대 속도로 돈다
        want_gibps = None
        lookup_keys = None

    keys_per_round = in_flight * num_keys
    round_bytes = keys_per_round * data_size

    # load 가 읽을 데이터셋의 크기. 키는 이 범위 안에서 돈다.
    dataset_keys = args.dataset_rounds * cal["shape"]["load_num_keys"]

    # 계획대로 도는 store 는 실제처럼 매번 새 key 를 쓰므로 load 데이터셋과 다른 폴더에 쌓는다.
    # 다 쓴 뒤 지우는 것은 부르는 쪽이 한다.
    paced_store = args.mode == "run" and op == "store"
    if not args.data_dir:          # 폴더는 부르는 쪽(hw_probe · replay)이 클러스터 설정에서 정한다
        raise SystemExit("--data-dir 를 준다" + (" (계획대로 도는 store 는 load 데이터셋과 다른 폴더)" if paced_store else ""))
    data_dir = args.data_dir
    os.makedirs(data_dir, exist_ok=True)

    # L1 버퍼는 실제로 쓰는 방향만큼만 잡는다. calibrate/load 는 같은
    # 프로세스에서 store 로 데이터를 깔고 읽으므로 두 벌이 필요하고,
    # 나머지는 한 벌이면 된다. in_flight 를 올리면 이 차이가 수십 GiB 다.
    needs_both = args.mode == "calibrate" and op == "load"
    load_base = round_bytes if needs_both else 0
    l1_buffer = lm["make_aligned_tensor"]((2 if needs_both else 1) * round_bytes, align)
    l1_desc = lm["create_l1_memory_desc"](l1_buffer, align_bytes=align)
    adapter = build_adapter(lm, adapter_json(cal, data_dir, args.backend_param), l1_desc)

    # 라운드 r 이 쓰는 key idx. 데이터셋 안에서 순환시켜 용량을 묶어둔다.
    slots = max(1, dataset_keys // keys_per_round)

    def keys_for_round(r):
        if paced_store:
            base = STORE_BASE + args.key_base + r * keys_per_round
            return [lm["make_object_keys"](num_keys, key_offset=base + i * num_keys)
                    for i in range(in_flight)]
        base = args.key_base + (r % slots) * keys_per_round
        return [lm["make_object_keys"](num_keys, key_offset=base + i * num_keys)
                for i in range(in_flight)]

    pacer = Pacer(period, args.phase) if period else None

    # 적중률 — 실제 경로처럼 라운드마다 lookup 을 먼저 하고 찾은 key 만 load 한다.
    # lookup 묶음 = 적중 key(데이터셋 안) + 미적중 key(라운드마다 새 idx, 디스크에 없음).
    # 미적중은 디스크 확인(stat)까지 가므로 metadata 부하도 실제처럼 생긴다.
    # load 시간 측정(t0) 앞에서 하므로 load 수치에는 섞이지 않는다.
    miss_per_round = (lookup_keys - num_keys) if (lookup_keys and op == "load") else 0
    lookups = []           # 라운드별 (초, 찾은 수, 요청 수)
    locked = []

    def lookup_round(r, hit_batches):
        if locked:
            adapter.submit_unlock(locked.pop())
        miss = lm["make_object_keys"](
            miss_per_round, key_offset=MISS_BASE + args.key_base + r * miss_per_round)
        keys = [k for b in hit_batches for k in b] + miss
        t = time.perf_counter()
        tid = adapter.submit_lookup_and_lock_task(keys, {0: lm["layout_desc"]})
        res = lm["wait_lookup_finished"](adapter, [tid], 60.0)
        found = lm["bitmap_count"](res.get(tid))
        lookups.append((time.perf_counter() - t, found, len(keys)))
        locked.append([k for b in hit_batches for k in b])

    def paced_keys(r):
        if pacer:
            pacer.wait_for(r)
        batches = keys_for_round(r)
        if miss_per_round:
            lookup_round(r, batches)
        return batches

    store_objs = load_objs = None

    def objs_for(fill_offset, which):
        def build(_r):
            nonlocal store_objs, load_objs
            cached = store_objs if which == "store" else load_objs
            if cached is None:
                base = 0 if which == "store" else load_base
                cached = [lm["make_memory_objects"](l1_buffer, num_keys, data_size,
                                                    base + i * num_keys * data_size,
                                                    fill_offset=fill_offset)
                          for i in range(in_flight)]
                if which == "store":
                    store_objs = cached
                else:
                    load_objs = cached
            return cached
        return build

    def log(msg):
        if not args.quiet:
            print(msg, flush=True)

    def wait_for_start():
        """여러 대·여러 프로세스의 출발을 맞춘다.

        adapter 생성과 index 패스는 걸리는 시간이 제각각이라, 그 뒤에
        기다려야 실제로 같이 출발한다.
        """
        if not args.start_at:
            return
        gap = args.start_at - time.time()
        if gap > 0:
            log(f"[sync] {gap:.2f}초 뒤 출발")
            time.sleep(gap)
        elif gap < -1.0:
            log(f"[sync] 출발 시각이 {-gap:.2f}초 지났다 — 바로 시작한다")

    rounds = args.rounds
    if args.duration and period:
        rounds = max(1, int(args.duration / period))

    out = {
        "host": socket.gethostname(),
        "mode": args.mode,
        "op": op,
        "started_at": time.time(),
        "shape": {"num_keys": num_keys, "in_flight": in_flight,
                  "data_size_kb": data_size // 1024, "l1_align_bytes": align,
                  "round_bytes": round_bytes},
        "backend_param_overrides": args.backend_param,
        "dataset": {"dir": data_dir, "rounds": args.dataset_rounds,
                    "keys": dataset_keys, "slots": slots},
        "requested_gibps": want_gibps,
        "round_period_s": period,
    }

    try:
        results = {}
        if args.mode == "prepare" or (args.mode == "calibrate" and op == "load"):
            # load 가 읽을 데이터를 같은 idx 범위에 먼저 써 둔다.
            n = args.dataset_rounds if args.mode == "prepare" else rounds + args.warmup
            log(f"[prepare] {n} 라운드 x {round_bytes / GIB:.2f} GiB 를 쓴다")
            results["prepare"] = lm["bench_store"](
                adapter, in_flight=in_flight, num_keys=num_keys,
                data_size=data_size, rounds=n,
                keys_for_round=keys_for_round,
                objs_for_round=objs_for(0, "store"), log=log)

        if args.mode == "run" and op == "load":
            # 새 프로세스의 adapter 는 디스크에 있는 key 를 모른다. load 는
            # in-memory index 만 보고(`_execute_load_in_loop`), 그 index 를
            # 디스크에서 채우는 것은 lookup 이다(`_secondary_lookup_locked`).
            # 실제 경로도 load 앞에 lookup 이 오므로 순서가 같다.
            log(f"[index] 데이터셋 {slots} 라운드를 lookup 으로 훑는다")
            idx = lm["bench_lookup"](
                adapter, in_flight=in_flight, num_keys=num_keys,
                rounds=slots, keys_for_round=keys_for_round, log=log)
            found = sum(idx.success_counts)
            if found < slots * keys_per_round:
                raise SystemExit(
                    f"데이터셋이 모자란다: {found}/{slots * keys_per_round} key 만 있다. "
                    f"--mode prepare --dataset-rounds {args.dataset_rounds} "
                    f"--key-base {args.key_base} 를 먼저 돌린다 (디렉터리 {data_dir})")
            log(f"[index] {found} key 확인")

        if args.mode in ("run", "calibrate"):
            wait_for_start()
            total = rounds + args.warmup
            log(f"[{op}] {args.warmup} warmup + {rounds} 라운드"
                + (f", 주기 {period:.2f}초" if period else ", 제한 없음"))
            fn = lm["bench_load"] if op == "load" else lm["bench_store"]
            results[op] = fn(
                adapter, in_flight=in_flight, num_keys=num_keys,
                data_size=data_size, rounds=total,
                keys_for_round=paced_keys,
                objs_for_round=objs_for(1 if op == "load" else 0, op),
                log=log)

        for name, res in results.items():
            durations = res.round_durations[args.warmup:] if name == op else res.round_durations
            busy = sum(durations)
            out[name] = {
                "rounds": len(durations),
                "timed_out_rounds": res.timed_out_rounds,
                "round_durations_s": durations,
                "busy_s": busy,
                "peak_gibps": (round_bytes / GIB) / min(durations) if durations else None,
                "mean_busy_gibps": (len(durations) * round_bytes / GIB) / busy if busy else None,
            }
        if lookups:
            lk = lookups[args.warmup:]
            out["lookup"] = {
                "keys_per_round": lookups[0][2],
                "hit_rate_planned": num_keys / lookups[0][2],
                "hit_rate_observed": sum(f for _, f, _ in lk) / sum(n for _, _, n in lk),
                "round_durations_s": [d for d, _, _ in lk],
                "found": [f for _, f, _ in lk],
            }
        if pacer:
            # 시각표는 warmup 라운드부터 걸려 있으므로 분자도 전부를 센다.
            # 분모는 라운드 N 개가 차지하는 시각표 창(N x 주기)이다. 마지막
            # 라운드가 끝난 시각에서 끊으면 주기 하나가 덜 들어가 부풀려진다.
            # 라운드가 밀려 창을 넘겼으면 실제 경과 시간을 쓴다.
            paced_rounds = rounds + args.warmup
            elapsed = max(time.perf_counter() - pacer.start - pacer.phase,
                          paced_rounds * period)
            out["offered"] = {
                "wall_s": elapsed,
                "paced_rounds": paced_rounds,
                "achieved_gibps": (paced_rounds * round_bytes / GIB) / elapsed if elapsed else None,
                "late_rounds": pacer.late_rounds,
                "idle_s": pacer.total_wait_s,
            }
    finally:
        try:
            adapter.close()
        except Exception:
            pass

    return out



def replay(args) -> dict:
    """plan.py 가 낸 event 를 시각표대로 replay 한다 (--mode replay).

    이벤트 하나 = 요청 하나. 실제 LMCache MP 서버 순서를 따른다.
      도착 → L1 prefix 뒤 key 전부 lookup (없는 key 는 adapter 가 3FS 에 stat) → 찾은 key 중 plan 의 load key 를 load
      → 찾은 key 모두 unlock → prefill 단계별 store 묶음을 보낸다
    lookup·load 는 요청 --slots 개(LMCache prefetch 동시 한도 8)까지 겹친다. 슬롯이 모자라면 요청이 늦게 시작하고,
    그 늦음을 센다. store 는 기다리지 않고 개수 제한 없이 겹친다(LMCache store controller 와 같다).
    load 는 한 번에 최대 --max-batch chunk.
    """
    # 클라이언트마다 계정이 다르다(spark4 는 pty, 나머지는 spark). adapter 는 읽을 때도 파일을
    # 쓰기 권한으로 열어서, 만든 계정이 아니면 load 가 PermissionError 로 실패한다. 공유 폴더의
    # 재생 파일은 누구나 열 수 있게 만든다
    os.umask(0)
    cal = json.loads(Path(args.calibration).read_text())
    apply_env(cal)
    lm = load_lmcache()
    if args.load_rdonly:
        # adapter 는 load 할 파일도 O_RDWR 로 연다. 3FS 에서 쓰기 open 은 파일마다 meta 왕복(약 5 ms, 읽기 전용은 0.01 ms)이고
        # event loop 에서 chunk 마다 차례로 돈다. 읽기 전용으로 바꿔 그 몫을 잰다 (LMCache 소스는 그대로, 이 프로세스에서만)
        from lmcache.v1.distributed.l2_adapters.nixl_store_agents.file_dynamic_nixl_store_agent import (
            FileDynamicNixlStorageAgent as Agent)
        rw = Agent._open_flags
        Agent._open_flags = lambda self, create: rw(self, create) if create else rw(self, create) & ~os.O_RDWR
    plan = json.loads(Path(args.schedule).read_text())
    events = plan["events"][args.client_index]
    prepop = plan["prepopulate"][args.client_index]
    if args.procs > 1:
        # 한 클라이언트의 event 를 프로세스 --procs 개가 나눠 맡는다 (adapter 의 프로세스 단위 한계를 걷는다).
        # 같은 대화는 같은 프로세스로 — store 한 key 를 바로 뒤 request 가 같은 adapter 의 index 에서 찾는다
        events = [e for e in events if (e[4] if len(e) > 4 else 0) % args.procs == args.proc_index]
        prepop = prepop[args.proc_index::args.procs]
    size = plan["chunk_bytes"]
    align = args.l1_align or cal["shape"]["l1_align_bytes"]
    slots_n = args.slots
    # prefetch 들이기: slots = 동시 --slots 개(LMCache 8d7d2c4 까지), watermark = L1 바이트 기준(a6accee6, 10-21 부터 — plan 이 정한다).
    #   watermark: 들어와 있는 요청이 없거나, (staging + lookup 중 최악) < watermark x L1 이고 L1 남은 자리 - lookup 중 최악 >= 이 요청의 최악.
    #   staging = 읽는 중인 load + 진행 중인 store, 최악 = 묻는 chunk 를 모두 L2 에서 읽는 바이트 (prefetch_controller._can_admit_new_prefetch)
    adm = plan.get("admission", "slots")
    l1_total = plan.get("l1_bytes", 0)
    wm_bytes = plan.get("prefetch_watermark", 0.5) * l1_total
    # --max-batch 0: 나누지 않는다 — LMCache prefetch 처럼 request 하나의 load 가 task 하나. 내용은 안 보니 슬롯이 load 영역 하나를 함께 쓴다.
    # watermark 면 동시 요청 수가 정해지지 않아 load 영역 하나를 함께 쓴다
    shared = args.max_batch == 0 or adm == "watermark"
    mb = max([len(e[2]) for e in events] + [1]) if args.max_batch == 0 else args.max_batch
    sb = max([len(b) for e in events for b in e[3]], default=1)
    os.makedirs(args.data_dir, exist_ok=True)

    # L1 버퍼: 슬롯마다 load 영역 하나 + store 영역 하나 (store 는 읽기만 하니 모든 store 가 함께 쓴다)
    region = mb * size
    n_regions = 1 if shared else slots_n
    l1_buffer = lm["make_aligned_tensor"](n_regions * region + sb * size, align)
    l1_desc = lm["create_l1_memory_desc"](l1_buffer, align_bytes=align)
    # {rdir}: 마운트를 거치지 않는 adapter(raw_hf3fs)는 3FS 루트 기준 경로를 받는다
    spec = (args.adapter_json.replace("{dir}", args.data_dir)
            .replace("{rdir}", os.path.relpath(args.data_dir, cal["adapter"]["backend_params"]["mount_point"]))
            if args.adapter_json else adapter_json(cal, args.data_dir, args.backend_param))
    adapter = build_adapter(lm, spec, l1_desc)
    mk = lm["make_object_keys"]
    keys_of = lambda idxs: [mk(1, key_offset=i)[0] for i in idxs]
    STORE = -1                                   # store 영역(모든 store 가 함께 쓴다)
    objs_of = lambda slot, n: lm["make_memory_objects"](l1_buffer, n, size,
                                                        n_regions * region if slot == STORE else 0 if shared else slot * region)

    def log(msg):
        if not args.quiet:
            print(msg, flush=True)

    import hashlib
    import lmcache
    pkg = Path(lmcache.__file__).parent
    tree = hashlib.sha256()
    for p in sorted(x for x in pkg.rglob("*") if x.suffix in (".py", ".so") and "__pycache__" not in x.parts and "tests" not in x.parts):
        tree.update(str(p.relative_to(pkg)).encode() + p.read_bytes())
    # 실제로 돈 코드의 지문. replay.py 가 클라이언트끼리 다르면 실행을 무효로 한다 (10-21: 멈춤 한도가 spark4 의 driver 에만 있었다)
    code = {"driver": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()[:12],
            "calibration": hashlib.sha256(Path(args.calibration).read_bytes()).hexdigest()[:12],
            "lmcache": tree.hexdigest()[:12]}
    out = {"host": socket.gethostname(), "mode": "replay", "client_index": args.client_index,
           "lmcache": str(pkg), "code": code, "l1_align": align,
           "trace": plan["trace"], "speed": plan["speed"], "events": len(events),
           "prepopulate_chunks": len(prepop), "admission": adm, "prefill_tps": args.prefill_tps}
    store_done = {}

    STALL_S = 180      # 이만큼 아무 것도 끝나지 않으면 멈춘 것으로 본다 (10-21: 먼저 끝난 클라이언트가 남의 store 를 깨뜨림 · 3FS 가 참)

    def wait_store(tids):
        pending, t0 = set(tids), time.perf_counter()
        while pending:
            store_done.update(adapter.pop_completed_store_tasks())
            pending -= store_done.keys()
            if pending:
                if time.perf_counter() - t0 > STALL_S:
                    raise RuntimeError(f"미리 쓰기 store {len(pending)} 개가 {STALL_S}초 동안 끝나지 않는다")
                time.sleep(0.001)

    try:
        # 구간 전에 저장돼 있어야 할 chunk 를 먼저 써 둔다 (시각표 밖)
        t = time.perf_counter()
        if args.keep_prepop:
            # 앞 실행이 남겨 둔 chunk 는 다시 쓰지 않는다 (replay.py --keep-prepop). lookup 으로 있는지 묻고 없는 것만 쓴다
            have = set()
            for i in range(0, len(prepop), 256):
                b = prepop[i:i + 256]
                keys = keys_of(b)
                tid = adapter.submit_lookup_and_lock_task(keys, {0: lm["layout_desc"]})
                while (res := adapter.query_lookup_and_lock_result(tid)) is None:
                    time.sleep(0.001)
                found = [k for j, k in enumerate(keys) if res.test(j)]
                if found:
                    adapter.submit_unlock(found)
                have.update(x for j, x in enumerate(b) if res.test(j))
            prepop = [x for x in prepop if x not in have]
        for i in range(0, len(prepop), min(mb, 64)):
            b = prepop[i:i + min(mb, 64)]
            wait_store([adapter.submit_store_task(keys_of(b), objs_of(0, len(b)))])
        store_done.clear()
        out["prepopulate_s"] = time.perf_counter() - t
        out["prepopulate_written"] = len(prepop)
        log(f"[replay] 미리 쓴 chunk {len(prepop)} ({out['prepopulate_s']:.1f}초)")

        if args.start_at:
            gap = args.start_at - time.time()
            out["start_gap_s"] = gap
            if gap > 0:
                time.sleep(gap)
        base = time.perf_counter()

        slots = [None] * slots_n if adm == "slots" else []      # lookup·load 중인 요청
        stores = {}                         # 진행 중인 store: task id -> (보낸 시각, chunk 수)
        later = []                          # GPU 계산을 기다리는 store 묶음 (--prefill-tps): (보낼 시각, chunk 들)

        def new_slot(ev, now):
            return {"due": ev[0], "start": now, "look": ev[1], "want": set(ev[2]), "batches": ev[3],
                    "after": ev[6] if len(ev) > 6 else [0] * len(ev[3]),
                    "loadq": None, "found": [], "task": None, "lookup_s": 0.0, "loaded": 0, "pending": 0}

        def can_admit(ev):
            live = [x for x in slots if x]
            if not live:
                return True
            lookup_b = sum(len(x["look"]) for x in live if x["loadq"] is None) * size
            staging = (sum(x["pending"] for x in live) + sum(n for _, n in stores.values())) * size
            used = (ev[5] if len(ev) > 5 else 0) * l1_total + staging
            return staging + lookup_b < wm_bytes and l1_total - used - lookup_b >= len(ev[1]) * size
        nxt = 0
        records, store_records = [], []
        missed = max_stores = 0
        layout = lm["layout_desc"]
        last_prog = time.perf_counter()
        # 끝 시각은 그 자리에서 다시 잰다 — adapter 가 lock 을 쥐고 동기 stat 을 하는 동안 adapter 호출이 막혀 루프 첫머리의 now 가 낡는다
        while nxt < len(events) or any(slots) or stores or later:
            now = time.perf_counter() - base
            progressed = False
            # 때가 된 요청을 들인다 — 빈 슬롯에(slots) 또는 L1 바이트가 허락하는 만큼 차례대로(watermark)
            if adm == "slots":
                for si in range(slots_n):
                    if slots[si] is None and nxt < len(events) and events[nxt][0] <= now:
                        slots[si] = new_slot(events[nxt], now)
                        nxt += 1
                        progressed = True
            else:
                slots = [x for x in slots if x]
                while nxt < len(events) and events[nxt][0] <= now and can_admit(events[nxt]):
                    slots.append(new_slot(events[nxt], now))
                    nxt += 1
                    progressed = True
            # GPU 계산이 끝난 store 묶음을 보낸다
            while later and later[0][0] <= now:
                _, b = later.pop(0)
                stores[adapter.submit_store_task(keys_of(b), objs_of(STORE, len(b)))] = (time.perf_counter() - base, len(b))
                max_stores = max(max_stores, len(stores))
                progressed = True
            # 끝난 store
            for tid, r in adapter.pop_completed_store_tasks().items():
                t0, n = stores.pop(tid)
                store_records.append({"t": t0, "n": n, "s": time.perf_counter() - base - t0, "ok": bool(r.is_successful())})
                progressed = True
            for si, sl in enumerate(slots):
                if sl is None:
                    continue
                task = sl["task"]
                if task is None:
                    if sl["loadq"] is None:                    # 도착: lookup
                        if not sl["look"]:
                            sl["loadq"] = []
                            continue
                        keys = keys_of(sl["look"])
                        sl["task"] = ("lookup", adapter.submit_lookup_and_lock_task(keys, {0: layout}), keys)
                    elif sl["loadq"]:                          # 찾은 load key 를 max-batch 씩
                        keys = sl["loadq"].pop(0)
                        sl["task"] = ("load", adapter.submit_load_task(keys, objs_of(si, len(keys))), keys)
                    else:                                      # load 끝: unlock, store 를 보내고 슬롯을 비운다
                        if sl["found"]:
                            adapter.submit_unlock(sl["found"])
                        if args.prefill_tps:                   # prefill 단계마다 GPU 가 그만큼 계산한 뒤에 그 단계의 store 를 보낸다
                            t_done = time.perf_counter() - base
                            later.extend((t_done + a / args.prefill_tps, b) for b, a in zip(sl["batches"], sl["after"]))
                            later.sort(key=lambda x: x[0])
                        else:
                            for b in sl["batches"]:
                                stores[adapter.submit_store_task(keys_of(b), objs_of(STORE, len(b)))] = (time.perf_counter() - base, len(b))
                        max_stores = max(max_stores, len(stores))
                        records.append({"due": sl["due"], "late": sl["start"] - sl["due"], "load_s": time.perf_counter() - base - sl["start"],
                                        "lookup_s": sl["lookup_s"], "looked": len(sl["look"]), "found": len(sl["found"]),
                                        "loaded": sl["loaded"], "want": len(sl["want"]),
                                        "stored": sum(len(b) for b in sl["batches"])})
                        missed += len(sl["want"]) - sl["loaded"]
                        slots[si] = None
                    progressed = True
                    continue
                kind, tid, keys = task
                res = (adapter.query_lookup_and_lock_result(tid) if kind == "lookup"
                       else adapter.query_load_result(tid))
                if res is None:
                    continue
                if kind == "lookup":
                    sl["lookup_s"] = time.perf_counter() - base - sl["start"]
                    sl["found"] = [k for i, k in enumerate(keys) if res.test(i)]
                    ld = [k for k in sl["found"] if k.kv_rank in sl["want"]]
                    sl["loadq"] = [ld[i:i + mb] for i in range(0, len(ld), mb)]
                    sl["pending"] = len(ld)
                else:
                    sl["loaded"] += sum(1 for i in range(len(keys)) if res.test(i))
                    sl["pending"] -= len(keys)
                sl["task"] = None
                progressed = True
            if progressed:
                last_prog = time.perf_counter()
            else:
                if time.perf_counter() - last_prog > STALL_S:
                    out["stalled"] = {"stores": len(stores), "slots": sum(1 for x in slots if x), "events_left": len(events) - nxt,
                                      "later": len(later)}
                    log(f"[replay] {STALL_S}초 동안 진행이 없어 멈춘다: {out['stalled']}")
                    break
                time.sleep(0.0005)
        out["wall_s"] = time.perf_counter() - base
        out["records"] = records
        out["store_records"] = store_records
        out["max_outstanding_stores"] = max_stores
        out["missed_loads"] = missed
    finally:
        # adapter 는 닫을 때 저장 폴더의 *.tmp.* 를 모두 지운다(다른 프로세스가 쓰는 중인 것까지). 클라이언트가
        # 끝나는 시각이 달라서 먼저 끝난 쪽이 남의 store 를 깨뜨린다(rename ENOENT). 폴더는 replay.py 가 비운다
        try:
            if hasattr(adapter, "nixl_agent"):
                adapter.nixl_agent.cleanup = lambda: None
            adapter.close()
        except Exception:
            pass
    return out

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", choices=["prepare", "run", "calibrate", "replay"], required=True,
                    help="prepare: load 가 읽을 데이터셋을 만든다 / "
                         "run: 계획된 양(없으면 제한 없이)으로 구동한다 — hw_probe.py 가 쓴다 / "
                         "calibrate: 제한 없이 최대를 잰다 / replay: plan.py 의 plan 을 replay 한다")
    ap.add_argument("--plan", help="run 을 정해진 양으로 돌릴 때의 계획 JSON (주지 않으면 제한 없이)")
    ap.add_argument("--calibration", default=str(HERE / "calibration.json"))
    ap.add_argument("--l1-align", type=int, default=None, help="L1 정렬 바이트 (기본: calibration 의 shape.l1_align_bytes 8 MiB. LMCache MP 기본은 4 KiB)")
    ap.add_argument("--op", choices=["load", "store"], default="load")
    ap.add_argument("--num-keys", type=int, default=None)
    ap.add_argument("--data-size-kb", type=int, default=None,
                    help="객체 하나의 크기. 기본은 calibration 의 shape")
    ap.add_argument("--in-flight", type=int, default=1)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--warmup", type=int, default=1)
    ap.add_argument("--duration", type=float, default=None,
                    help="초. 주기에서 라운드 수를 계산한다 (run 전용)")
    ap.add_argument("--dataset-rounds", type=int, default=8,
                    help="load 가 읽을 데이터셋을 load 라운드 몇 개 분량으로 둘지")
    ap.add_argument("--data-dir", default=None)
    ap.add_argument("--backend-param", action="append", default=[], metavar="K=V",
                    help="adapter 의 backend_params 를 덮어쓴다 (여러 번 쓸 수 있다)")
    ap.add_argument("--key-base", type=int, default=0,
                    help="이 프로세스가 쓸 key idx 의 시작. 한 클라이언트에서 "
                         "여러 프로세스를 띄울 때 범위를 겹치지 않게 한다")
    ap.add_argument("--phase", type=float, default=0.0,
                    help="초. 클라이언트마다 다르게 주어 합계를 고르게 만든다")
    ap.add_argument("--start-at", type=float, default=None,
                    help="epoch 초. 여러 대의 출발을 맞춘다")
    ap.add_argument("--out", default=None)
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--schedule", help="plan.py 가 낸 plan (replay 전용)")
    ap.add_argument("--client-index", type=int, default=0, help="계획에서 이 클라이언트의 번호 (replay 전용)")
    ap.add_argument("--slots", type=int, default=8, help="lookup·load 를 동시에 진행하는 요청 수 — LMCache prefetch 한도 (replay 전용)")
    ap.add_argument("--procs", type=int, default=1, help="이 클라이언트의 event 를 나눠 맡는 프로세스 수 (replay 전용)")
    ap.add_argument("--proc-index", type=int, default=0, help="그중 이 프로세스의 번호 (replay 전용)")
    ap.add_argument("--max-batch", type=int, default=64, help="load 한 번에 보낼 최대 chunk 수, 0 이면 request 당 task 하나 (replay 전용)")
    ap.add_argument("--prefill-tps", type=float, default=0,
                    help="GPU prefill 속도(token/s). 주면 load 가 끝난 뒤 prefill 단계마다 그만큼 기다렸다가 그 단계의 store 를 보낸다 (replay 전용, 0 이면 바로)")
    ap.add_argument("--load-rdonly", action="store_true", help="load 할 파일을 읽기 전용으로 연다 (replay 전용)")
    ap.add_argument("--adapter-json", default=None,
                    help="calibration 의 adapter 대신 쓸 adapter JSON. {dir} 는 --data-dir 로 바뀐다 (replay 전용). 예: hf3fs")
    ap.add_argument("--keep-prepop", action="store_true",
                    help="미리 쓸 chunk 중 이미 있는 것은 건너뛴다 (replay 전용)")
    args = ap.parse_args()

    if args.mode == "replay":
        out = replay(args)
        text = json.dumps(out, ensure_ascii=False)
        if args.out:
            Path(args.out).write_text(text)
        return

    out = run(args)
    text = json.dumps(out, ensure_ascii=False, indent=2)
    if args.out:
        Path(args.out).write_text(text)
    if not args.quiet:
        for name in ("prepare", "load", "store"):
            if name in out and out[name]["rounds"]:
                s = out[name]
                print(f"\n{name:8} 라운드 {s['rounds']}  "
                      f"평균 {s['mean_busy_gibps']:.3f} GiB/s  "
                      f"최고 {s['peak_gibps']:.3f} GiB/s")
        if "lookup" in out:
            lk = out["lookup"]
            print(f"{'lookup':8} 적중률 계획 {lk['hit_rate_planned']:.1%}  "
                  f"실제 {lk['hit_rate_observed']:.1%}  "
                  f"평균 {1000 * sum(lk['round_durations_s']) / len(lk['round_durations_s']):.0f} ms")
        if "offered" in out:
            o = out["offered"]
            print(f"{'제공':8} 요청 {out['requested_gibps']:.3f} GiB/s  "
                  f"실제 {o['achieved_gibps']:.3f} GiB/s  "
                  f"늦은 라운드 {o['late_rounds']}  쉰 시간 {o['idle_s']:.1f}초")
    if not args.out:
        print()


if __name__ == "__main__":
    main()
