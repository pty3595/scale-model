#!/usr/bin/env python3
"""손잡이 목록의 원본. `python3 knobs.py` 가 KNOBS.md 를 다시 낸다 — KNOBS.md 는 손으로 고치지 않는다.

목록은 기억이 아니라 소스의 설정 항목을 훑어 만든다(10-21: LMCache MP 서버 flag · L2 adapter 설정,
3FS FuseConfig · StorageClient · StorageOperator · Components · AioReadWorker · BufferPool · IBSocket).
IO 경로에 있는 항목은 모두 여기 있어야 한다 — 도구가 도는 것(sweep), 아직 안 잰 후보(todo), 뺀 것(out), 상황 입력 · 하드웨어(fixed).

손잡이 하나의 칸
  id · layer · key   이름 · 층 · 코드나 설정 파일에서 찾을 문자열
  what · how         실제 의미 · 어디서 어떻게 바꾸나(바로 / 재시작 / 폴더마다 / adapter JSON / MP 서버 flag)
  base               기준선 (값, 출처) — 소스 기본값이나 문서 예시
  ours               이 클러스터에서 쓰는 값과 까닭. 기준선과 같으면 None
  cands              후보 [(이름, 값)]. 이름은 결과 폴더의 tag 와 같다 — tune.py 의 adapter 설정 · 클러스터 설정, screen.py 의 후보
  when               언제 움직이나 — 다른 서버에도 통하는 이유(측정이 아니라 기구)
  cost               비용 · 위험
  tier · why         sweep · todo · out · fixed 와 그 까닭
측정 숫자는 손으로 적지 않는다. 아래 SITUATIONS 의 결과 폴더에서 끌어온다(render).
"""
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE.parent / "results/tune"
GRID = [0.03, 0.04, 0.05, 0.07, 0.10, 0.14, 0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50]   # 10-27 전 tune.py 의 단계 격자 — OUT 의 결과는 단계로 쟀다


def K(id, layer, key, what, how, base, tier, ours=None, cands=(), when="", cost="", why=""):
    return dict(id=id, layer=layer, key=key, what=what, how=how, base=base, ours=ours, cands=list(cands),
                when=when, cost=cost, tier=tier, why=why)


KNOBS = [
    # --- LMCache MP 서버 ---
    K("l1-align", "LMCache MP 서버", "--l1-align-bytes", "L1 메모리 정렬", "MP 서버 flag (replay 는 --l1-align)",
      ("4 KiB", "LMCache docs/source/mp/configuration.rst"), "sweep",
      ours="replay 만 8 MiB(calibration.json shape) — 09-22 gate 실험(NIXL dram, 40 MiB)에서 4 KiB 2.66 · 8 MiB 3.07 GiB/s 라 골랐다",
      cands=[("s16-d16-a4k", "4 KiB (hf3fs)"), ("dram_zc-a4k", "4 KiB (nixl_store_dynamic)"), ("dram_zc-a2m", "2 MiB (nixl_store_dynamic)"), ("w16-a4k", "4 KiB (nixl_native)")],
      when="replay 에서는 버퍼 시작 주소와 adapter 에 넘기는 align_bytes 만 바뀐다(key 는 정렬 없이 이어 붙인다). "
           "L1 을 그대로 NIXL 에 등록하는 dram_zc 에서만 움직인다 — hf3fs(복사) · nixl_native(dram)는 같다. key 크기가 정렬의 배수가 아니면 NIXL 등록이 깨진다(17 MiB key 에 8 MiB)",
      cost="dram_zc 에서 8 MiB · 2 MiB 가 4 KiB 보다 한 단계 낫다(2 MiB 면 충분 — huge page 경계로 본다). 정렬은 key 크기를 나눠야 한다 — 아니면 adapter 가 안 뜬다"
           "(DeepSeek-V3 17,989,632 B = 32 KiB x 549 → 2 MiB · 8 MiB 모두 NIXL_ERR_NOT_FOUND). 빠른 길은 key 가 2 MiB 배수인 모델만"),
    K("l1-size", "LMCache MP 서버", "--l1-size-gb, vLLM GPU KV", "L1 · GPU KV 크기", "상황 입력",
      ("대상 서버 값", "—"), "fixed", ours="L1 32 · GPU KV 24 GiB(Spark 한 대에 맞춘 가정)",
      why="최적화할 값이 아니다. L1 크기 하나로 load 양이 두 배 넘게 바뀐다 — L1 8 · 32 · 128 GiB 에 load 3.25 · 2.52 · 1.50 GiB/s, store 1.71 그대로(시뮬레이터, coder 30-33분 1배속 세 대 합)"),
    K("chunk-tokens", "LMCache MP 서버", "--chunk-size", "key 하나의 token 수", "상황 입력",
      ("256", "LMCache multiprocess/config.py"), "fixed",
      why="모델의 token 당 KV 바이트와 함께 key 크기를 정한다 — Qwen3-14B 40 MiB · DeepSeek-V3 17 MiB · Llama-70B 80 MiB"),
    K("prefetch", "LMCache MP 서버", "--l2-prefetch-max-in-flight", "load 를 몇 request 겹치나", "MP 서버 flag (replay 는 --slots)",
      ("8", "LMCache distributed/config.py"), "sweep",
      cands=[("s16-d16-sl4", "4 (폴더 16)"), ("s16-d16-sl16", "16 (폴더 16)"), ("s16-d1-sl4", "4 (폴더 1, Llama)"), ("s16-d1-sl16", "16 (폴더 1, Llama)")],
      when="load 를 몇 request 겹치나 — 읽기 동시성을 LMCache 쪽에서 바꾼다. replay 의 --slots"),
    K("eviction", "LMCache MP 서버", "--eviction-policy · --eviction-trigger-watermark · --eviction-ratio", "L1 을 언제 얼마나 비우나", "MP 서버 flag",
      ("LRU(필수 인자) · 0.8 · 0.2", "LMCache distributed/config.py, targets/serving-base.json"), "sweep",
      when="IO 경로가 아니라 IO 양(plan)을 바꾼다 — 도구는 `tune.py --evict 문턱/비율` 로 plan 을 시뮬레이터로 다시 낸다. 단계가 아니라 배속으로 견준다. "
           "coder x Qwen(hf3fs): 0.9/0.2 x3.96 · 0.8/0.1 x3.98 · 기본 x3.73 · 0.8/0.4 x3.54 · 0.7/0.2 x3.49 — 늦게 · 조금씩 비우면 load 가 6% 줄어 ±5-7%",
      cost="시뮬레이터로만 낸 IO 다. 실제 MP 서버와의 대조는 기본값(0.8/0.2)에서만 했다"),
    K("mp-workers", "LMCache MP 서버", "--max-workers · --l1-use-lazy", "MP 서버 worker 수 · L1 을 나눠 잡기", "MP 서버 flag",
      ("1 · lazy", "LMCache multiprocess/config.py"), "out",
      ours="시뮬레이터 대 실제 대조에서만 2 · lazy 끔(validate/launch_mp.py, 출처 기록 없음)",
      why="대조 실행에만 쓴다. 10-22 LMCache 기본으로 다시 대조: IO 일치 97.51% · load +5.5% · store · lookup 0% — 예전 flag 와 같다(MP_FLAGS=lmcache-default)"),

    # --- adapter: hf3fs (최신 버전 하나 — 원자적 쓰기 패치, 10-23 사용자). key 하나 = 파일 하나, worker 하나가 key 하나를 IO 한 번으로 ---
    K("folders", "adapter hf3fs", "base_paths", "폴더 수 — key hash 로 폴더 하나를 고른다", "adapter JSON",
      ("2", "LMCache hf3fs.rst 예시"), "sweep", ours="16",
      cands=[("s16-d1", "1"), ("doc", "2 (문서 예시 전체: worker 4 · lookup 2 · retrieve 16 · store 4)"), ("s16-d16", "16"), ("d32", "32"), ("d64", "64")],
      when="폴더마다 파일 만들기에 한도가 있다. key 가 작을수록 같은 GiB 에 파일이 많아 그 한도에 먼저 닿는다", cost="재지 못한 손해 없음"),
    K("store-workers", "adapter hf3fs", "per_op_workers.store", "동시에 쓰는 key 수", "adapter JSON",
      ("4", "LMCache hf3fs.rst 예시"), "sweep", ours="16",
      cands=[("w4-d1", "4 공용(폴더 1)"), ("s16-d1", "16(폴더 1)"), ("rw8", "8, retrieve 도 8"), ("rw32", "32, retrieve 도 32"), ("s8", "8"), ("s32", "32")],
      when="store 는 파일 만들기 · 쓰기 · rename 의 대기를 worker 수만큼 겹친다(추정). 16 에서 8 로 줄이면 key 가 작을 때만 무너진다 "
           "(DeepSeek 17 MiB 두 단계 넘게, Qwen · Llama 는 같은 밤 기준과 같다). 32 는 어디서도 오르지 않는다",
      cost="worker 하나가 iov 공유 메모리 200 MB 를 잡는다. Spark 는 GPU 와 메모리를 같이 써서 vLLM 몫이 준다"),
    K("retrieve-workers", "adapter hf3fs", "per_op_workers.retrieve", "동시에 읽는 key 수", "adapter JSON",
      ("16", "LMCache hf3fs.rst 예시"), "sweep", cands=[("r8", "8"), ("rw32", "32, store 도 32")],
      when="읽기는 FUSE 읽기 admission(클라이언트당 RPC 12)이 먼저 찬다 — 40 MiB key 하나가 storage 4대에 노드당 RPC 3개씩 12개를 낸다(코드 계산). 8 이면 충분하다",
      cost="store worker 와 같다"),
    K("lookup-workers", "adapter hf3fs", "per_op_workers.lookup", "동시에 있는지 보는 수 — 파일마다 stat", "adapter JSON",
      ("2", "LMCache hf3fs.rst 예시"), "sweep", cands=[("lk8", "8")], when="lookup 은 key 마다 meta 왕복이다. request 의 시작 늦음에 들어간다. 2 로 막히지 않는다"),
    K("shared-workers", "adapter hf3fs", "num_workers", "per_op_workers 에 없는 작업의 공용 worker", "adapter JSON",
      ("4", "LMCache hf3fs.rst 예시"), "out", why="세 작업을 따로 주면 delete 만 쓴다(adapter 문서). worker 마다 iov 200 MB 는 그대로 잡는 것으로 본다"),
    K("key-buffer", "adapter hf3fs", "enable_key_buffer", "key 를 노드 안 버퍼에 모은다", "adapter JSON",
      ("켬", "hf3fs_l2_adapter.py"), "fixed", ours="끈다", why="켜면 다른 노드가 서로의 key 를 못 찾는다 — 정확성"),
    K("usrbio-queue", "adapter hf3fs", "io_depth · ior_entries · iov_size", "USRBIO 큐 깊이 · 버퍼", "adapter JSON",
      ("기본", "hf3fs_l2_adapter.py"), "out", why="효과 없음(코드): worker 하나가 key 하나를 IO 한 번으로 하고, key 가 iov 200 MB 안에 든다"),

    # --- adapter: NIXL ---
    K("mem-config", "adapter NIXL", "backend_params.mem_config (nixl_store_dynamic)", "dram = IO 마다 복사, dram_zc = L1 을 그대로 등록, auto = plugin 이 고름", "adapter backend param",
      ("auto", "LMCache nixl.rst 예시는 mem_config 를 주지 않는다 = plugin 기본"), "sweep", ours="dram_zc",
      cands=[("auto", "auto"), ("dram", "dram"), ("dram_zc", "dram_zc")],
      when="auto 는 이 클러스터에서 dram 으로 내려앉는다(로그 hf3fs_backend.cpp:588). 복사가 없으면 클라이언트 한 대 벽이 3.06 → 5.37 GiB/s(09-23)",
      cost="dram_zc 는 2 GiB 넘는 버퍼 등록이 짧은 pwrite 로 실패하는 plugin 버그가 있다 — /tmp/pwrite_full.so 를 LD_PRELOAD(세 대 /tmp, 재부팅하면 사라짐)"),
    K("native-workers", "adapter NIXL", "num_workers (nixl_native)", "IO worker 수", "adapter JSON",
      ("4", "nixl_native_l2_adapter.py 기본 · 문서 예시"), "sweep", cands=[("w4", "4"), ("w16", "16")]),
    K("native-mem-config", "adapter NIXL", "backend_params.mem_config (nixl_native)", "dram = IO 마다 복사, dram_zc = L1 을 그대로 등록", "adapter backend param",
      ("auto", "LMCache nixl_native.rst 예시는 mem_config 를 주지 않는다 = plugin 기본"), "out", ours="dram",
      cands=[("w16", "dram"), ("w16-zc", "dram_zc (8 MiB 정렬)"), ("w16-zc-a4k", "dram_zc (4 KiB 정렬)")],
      when="nixl_store_dynamic 에서 dram → dram_zc 가 두 단계 넘게 올렸다", cost="dynamic 과 같은 2 GiB 버그 shim 이 필요하다",
      why="dram_zc 는 세 모델 모두 L1 등록에서 실패한다(NIXL_ERR_BACKEND, hf3fs_backend.cpp:588 No space left on device — /dev/shm 57 GB 여유에서도, shim 을 걸어도). 까닭은 가르지 않았다"),
    K("direct-io", "adapter NIXL", "backend_params.use_direct_io", "O_DIRECT 로 열기", "adapter backend param",
      ("필수 인자", "file_dynamic_nixl_store_agent.py"), "sweep", ours="true",
      cands=[("dram_zc-nodio", "false (nixl_store_dynamic dram_zc)"), ("w16-nodio", "false (nixl_native)")]),
    K("iopool", "adapter NIXL", "backend_params.iopool_size (nixl_store_dynamic)", "NIXL HF3FS plugin 의 IO 묶음 수", "adapter backend param",
      ("64", "calibration.json"), "out", why="09-22 gate 실험에서 64 · 128 · 256 · 512 가 같다(2.79-3.06 GiB/s, calibration.json invariant_to)"),

    # --- 3FS client (FUSE) 읽기. USRBIO 읽기도 FUSE 프로세스의 storage client 를 지난다 ---
    K("read-admission", "FUSE 읽기", "storage.traffic_control.read.max_concurrent_requests · _per_server", "클라이언트당 · storage 노드당 동시 읽기 RPC", "FUSE 템플릿, 바로 바뀜",
      ("32 / 8", "3FS StorageClient.h, upstream configs/hf3fs_fuse_main.toml"), "sweep", ours="12 / 3 (w38 09-14-15 sweep, 클라이언트 1대 · 1 MiB 읽기)",
      cands=[("src", "32 / 8 (소스 기본값 — 문서 예시 설정에서 잼, 쓰기는 64/32)"), ("src-w32", "32 / 8 · 쓰기도 32 / 8 (참 소스 기본값)"), ("r6", "6 / 2"), ("r32-1m", "32 / 8 · 묶음 1MB"),
             ("rts-r6", "6 / 2 · request-to-send"), ("rts-r8", "8 / 2 · request-to-send"), ("rts-r16", "16 / 4 · request-to-send"), ("rts-r24", "24 / 6 · request-to-send"), ("r32-rts", "32 / 8 · request-to-send")],
      when="동시에 떠 있는 읽기 바이트 ≈ admission x 묶음(기준 12 x 4 MB = 48 MB). 손실 스위치에서 여러 storage 가 한 클라이언트로 한꺼번에 보내면(incast) packet 이 버려진다"),
    K("read-batch", "FUSE 읽기", "storage.traffic_control.read.max_batch_bytes", "RPC 하나에 담는 최대 바이트 — chunk 보다 작아질 수 없다", "FUSE 템플릿, 바로 바뀜",
      ("4MB", "3FS StorageClient.h"), "sweep", cands=[("b1m", "1MB")],
      when="key 가 크면 RPC 마다 꽉 차고 admission 도 꽉 차 한꺼번에 몰리는 양이 가장 크다. 읽기 몰림이 줄면 store 가 쓸 몫이 생긴다(추정). "
           "몰림을 줄이는 세 방법(묶음 1MB · 32/8 · 1MB · request-to-send)이 큰 key 에서 모두 +2. coder x Qwen 은 같은 밤 셈으로 request-to-send 가 +1(0.40 4/4 대 기준 1/6), "
           "묶음 1MB 2/3, 32/8 · 1MB 1/3 — 몰림이 줄면 store 가 제때 끝나 못 찾음이 준다"),
    K("request-to-send", "FUSE 읽기", "enable_rdma_control + storage rdma_transmission_req_timeout", "storage 가 보내기 전에 클라이언트 허락을 받는다", "FUSE 템플릿 · storage, 바로 바뀜",
      ("끔 · 0", "3FS 소스"), "sweep", ours="켬 · 1s — 측정: 큰 key 에서 +1-2 단계, 잰 상황에서 나빠진 곳이 없다(nixl_native 는 안 잼). 실험 기준(clusters/spark.toml [ref])은 끈 채로 두고 후보로 잰다",
      cands=[("rts", "켬 · 1s"), ("r32-1m-rts", "32 / 8 · 1MB · 켬 (w41 의 손실 없는 읽기 설정)"), ("rts-b1m", "켬 · 묶음 1MB (겹침)")],
      when="몰림을 보내는 쪽에서 막는다. 손실이 없는 망이면 효과가 없다. 읽기 쪽 손잡이 중 가장 고르게 듣는다(Llama +2, coder x Qwen +1 — 같은 밤 0.40 4/4 대 기준 1/6)",
      cost="storage 가 보내기 전에 허락을 한 번 더 주고받는다. 템플릿(FUSE)과 storage 를 함께 바꾼다. "
           "storage 의 1s 는 hot-update 라 storage 를 다시 띄우면 0ns 로 돌아간다(그러면 끈 것과 같다 — 도구의 storage 확인이 실패로 알린다)"),
    K("rts-concurrency", "FUSE 읽기", "storage.net_client.rdma_control.max_concurrent_transmission", "request-to-send 를 켰을 때 클라이언트가 동시에 허락하는 storage 전송 수", "FUSE 템플릿, 바로 바뀜",
      ("64", "3FS RDMAControl.h"), "sweep", cands=[("r32-rts-mct16", "16 (읽기 32/8 · request-to-send)")],
      when="10-02 hw_probe: 64 · 16 · 4 · 2 가 같았다 — 읽기 admission 12/3 이 먼저 묶었다. request-to-send 를 켜고 admission 을 올리면 다시 묶일 수 있어 replay 로 본다(10-23 목록에 없던 것)"),
    K("usrbio-fuse", "FUSE 읽기", "batch_io_coros · io_worker_coros · max_jobs_per_ioring", "USRBIO 요청을 FUSE 가 처리하는 coroutine · 한 번에 꺼내는 수", "FUSE 템플릿",
      ("128 · 8 / 8 · 32", "3FS FuseConfig.h"), "out",
      why="보통 우선순위 coroutine 이 112개(128 - 8 - 8)로 adapter worker(MP 서버당 38개)보다 많아 상한이 아니다(코드 계산)"),
    K("read-target", "FUSE 읽기", "storage_io.read.targetSelection.mode", "복제 둘 중 어디서 읽나", "FUSE 템플릿, 바로 바뀜",
      ("Default = LoadBalance", "3FS TargetSelection.h · StorageClientImpl.cc"), "sweep",
      cands=[("ts-rr", "RoundRobin"), ("ts-rand", "RandomTarget"), ("ts-head", "HeadTarget"), ("rts-rand", "RandomTarget + request-to-send (겹침)")]),
    K("fuse-kernel", "FUSE 읽기", "max_readahead · enable_writeback_cache · io_bufs", "커널 FUSE 경로의 미리 읽기 · 버퍼", "FUSE 템플릿",
      ("기본", "3FS FuseConfig.h"), "out", why="USRBIO 는 커널 FUSE 경로를 타지 않는다"),
    K("fuse-threads", "FUSE 읽기", "storage.net_client.thread_pool · io_worker.num_event_loop", "FUSE storage client 의 IO · 처리 thread 와 event loop (읽기 · 쓰기 공통)", "FUSE 템플릿",
      ("io 2 · proc 2 · bg 2 · event loop 1", "3FS net Client 설정"), "out",
      why="10-23 replay 기록: 부하 중에도 FUSE 프로세스가 0.6 코어, 가장 바쁜 코어 14-18% — 상한이 아니다. 클라이언트 CPU 가 바빠지는 클러스터에서는 다시 본다"),
    K("tc-batch", "FUSE 읽기", "storage.traffic_control.*.max_batch_size · random_shuffle_requests · process_batches_in_parallel", "묶음 하나의 IO 수 · 요청 순서 섞기 · 묶음 병렬 (읽기 · 쓰기 공통)", "FUSE 템플릿",
      ("128 · 켬 · 켬", "3FS StorageClient.h"), "out",
      why="묶음 4MB 에 chunk 1 MiB 면 묶음당 IO 가 4개라 128 에 닿지 않는다(코드 계산). chunk 나 묶음 크기를 바꾸면 다시 본다"),
    K("retry-timeout", "FUSE 읽기", "storage.retry · storage_io.*.retry · default_timeout · compression · debug", "실패 뒤 다시 하기 · RPC timeout · 압축 · 시험용 주입 (읽기 · 쓰기 공통)", "FUSE 템플릿",
      ("기본", "3FS StorageClient.h · net Client 설정"), "out", why="실패 처리와 진단용이라 처리량 경로가 아니다. 압축은 0(끔)이고 RDMA 데이터에는 걸리지 않는다"),

    # --- 3FS client (FUSE) 쓰기 ---
    K("write-admission", "FUSE 쓰기", "storage.traffic_control.write.max_concurrent_requests · _per_server", "동시 쓰기 RPC", "FUSE 템플릿 + FUSE 재시작(CONFIG_ITEM, tune/fuse_write.py)",
      ("32 / 8", "3FS StorageClient.h, upstream configs/hf3fs_fuse_main.toml"), "sweep",
      ours="64 / 32 — 템플릿 값(출처 기록 없음)이지만 10-22 측정으로 선다: 32/8 이면 store 가 약 4-5 GiB/s 에서 막힌다",
      cands=[("w32", "32 / 8 (소스 기본값)"), ("w128", "128 / 64")],
      when="store 의 동시 쓰기 수. 32/8 이면 store 가 약 4-5 GiB/s(64/32 는 6.5-7.2)에서 막혀 api(B) · chat(A) -1 넘게, coder x Qwen 두 단계 넘게 나쁘다. "
           "버림(packet)은 1/20 로 준다 — 더 키우면(128/64) store 벽이 오를지가 다음 후보",
      cost="바꾸려면 클라이언트 세 대 FUSE 를 다시 띄워야 한다 — 그동안 3FS 마운트를 쓰는 것이 없어야 한다"),
    K("write-batch", "FUSE 쓰기", "storage.traffic_control.write.max_batch_bytes", "쓰기 RPC 하나에 담는 최대 바이트", "FUSE 템플릿 + FUSE 재시작(CONFIG_ITEM, StorageClient.h OperationConcurrency)",
      ("4MB", "3FS StorageClient.h"), "sweep", cands=[("wb1m", "1MB")],
      when="10-23 목록에 없던 것을 찾았다. 읽기 묶음 1MB 가 읽기 몰림을 줄였듯 쓰기 몰림을 줄일 후보로 쟀다. "
           "api(B) 10-23 같은 때(앞뒤 기준 네 번 · 1MB 세 번): 0.25 통과 2/3 대 1/4, store 달성 0.95 대 0.94 · 0.30 은 모두 실패, 0.81 대 0.78 — 단계는 같고 연속 값만 조금 낫다(흔들림 안). "
           "storage NIC 수신 버림은 줄지 않는다(초당 54만-57만 대 48만-50만). storage 가 chunk 마다 따로 당겨 오므로 묶음은 RPC 수만 바꾼다(추정)",
      cost="쓰기 admission 처럼 클라이언트 세 대 FUSE 를 다시 띄워야 한다"),
    K("write-checksum", "FUSE 쓰기", "storage_io.write.enableChecksum", "chunk 마다 CRC32C", "FUSE 템플릿",
      ("켬", "3FS StorageClient.h"), "out", why="무결성 — 끄지 않는다"),

    # --- storage ---
    K("st-rdma-writes", "storage", "server.storage.max_concurrent_rdma_writes", "storage 가 클라이언트에 써 주는 동시 RDMA(load 응답)", "storage, 바로 바뀜",
      ("256", "3FS StorageOperator.h"), "sweep", cands=[("st-load32", "32"), ("st-load8", "8"), ("st-load512", "512")]),
    K("st-rdma-reads", "storage", "server.storage.max_concurrent_rdma_reads", "storage 가 클라이언트에서 당겨 오는 동시 RDMA(store)", "storage, 바로 바뀜",
      ("256", "3FS StorageOperator.h"), "sweep", cands=[("st-store32", "32"), ("st-store64", "64"), ("st-store128", "128"), ("st-store512", "512"), ("st512-r32-1m", "512 + FUSE 읽기 32/8 · 1MB (겹침)"), ("st-both512", "512 · load 쪽도 512")],
      when="줄이면 store 가 막힌다. key 가 클수록 더. 512 로 늘려도 네 상황 모두 같은 밤 기준과 같다 "
           "(api(B) 의 \"+1\" 과 Qwen 의 \"-1\" 은 기준을 다른 때 잰 탓이었다). "
           "10-23 api(B): 64 · 128 은 같은 때 기준과 같다 — 0.25 통과 셋 다 1/3, store 달성 0.92-0.94, storage NIC 수신 버림도 같다(초당 44만-51만). "
           "storage 가 store 를 당겨 오는 동시 수(NIC 마다 semaphore, chunk 하나에 한 칸)를 1/4 로 줄여도 버림이 줄지 않는다 — store 벽은 이 한도가 정하지 않는다"),
    K("st-coro-update", "storage", "server.coroutines_pool_update", "쓰기 요청을 처리하는 coroutine", "storage, 바로 바뀜",
      ("64 / 8", "3FS CoroutinesPool.h · DynamicCoroutinesPool.h"), "out", why="10-19 128 · 256 효과 없음(results/settings-20261018, 예전 plan)"),
    K("st-coro-read", "storage", "server.coroutines_pool_read", "읽기 요청을 처리하는 coroutine", "storage, 바로 바뀜",
      ("64 / 8", "3FS DynamicCoroutinesPool.h"), "sweep", cands=[("cr128", "128 / 16"), ("cr256", "256 / 32")]),
    K("st-aio", "storage", "server.aio_read_worker (ioengine · num_threads · max_events)", "디스크 읽기 엔진", "storage (ioengine 만 바로)",
      ("libaio · 32 · 512", "3FS AioReadWorker.h"), "out",
      why="지금 최대 단계에서 디스크 읽기 사용률이 약 40% — 디스크가 막히지 않는다. 디스크까지 가는 상황이 오면 다시 본다"),
    K("st-buffers", "storage", "server.buffer_pool · storage.write_worker", "storage 의 RDMA 버퍼 · 쓰기 thread", "storage 재시작",
      ("4 MB x 1024 · 32", "3FS BufferPool.h · UpdateWorker.h"), "out", why="storage 를 다시 띄워야 한다 — 클러스터 기본값은 클러스터 관리자가 정한다"),

    # --- RDMA (클라이언트 · storage 양쪽) ---
    K("connections", "RDMA", "storage.net_client.io_worker.transport_pool.max_connections", "클라이언트 · storage 한 쌍의 연결(QP) 수 — 요청마다 무작위로 하나를 쓴다", "FUSE 템플릿, 바로 바뀜(새 연결은 필요할 때 만든다)",
      ("1", "3FS TransportPool.h"), "sweep", cands=[("conn2", "2"), ("conn4", "4")],
      when="연결 하나에 동시에 걸리는 RDMA READ 는 16(ConnectX 하드웨어 최대 = max_rd_atomic 16). storage 가 store 데이터를 당겨 오는 것도 클라이언트가 연 연결 위의 RDMA READ 라, "
           "연결이 하나면 쌍마다 16 에서 막힐 수 있다(추정) — storage 한도 64-256 이 듣지 않은 까닭 후보",
      cost="연결이 늘면 동시에 오는 양도 늘어 버림이 늘 수 있다. 줄여도 만든 연결은 남지만 쓰이지 않는다(무작위 번호가 0 만 나온다)",
      why="10-23 api(B) 같은 때 세 바퀴: **나쁘다.** store 달성 0.25 에서 0.90 → 0.81(2 · 4), 0.30 에서 0.78 → 0.74(2) · 0.61(4), 시작 늦음 p99 도 는다. "
          "storage NIC 수신 버림은 절반 아래로 준다(초당 42만 → 22만 · 18만) — 그런데 storage 쪽 RDMA 재전송(4.9만 → 6.3만 · 11.2만)과 클라이언트 순서 오류(4.3만 → 10.9만 · 23.6만)가 는다. "
          "잃는 곳이 storage NIC 에서 망 쪽으로 옮겨 갈 뿐이다 — NIC 버림은 store 벽의 원인이 아니라 손실 망의 한 증상이다(추정)"),
    K("rd-atomic", "RDMA", "io_worker.ibsocket.max_rd_atomic", "연결 하나에 동시에 걸리는 RDMA READ 수", "FUSE 템플릿 · storage, 새 연결부터",
      ("16", "3FS IBSocket.h"), "out", why="이미 NIC 하드웨어 최대다(ConnectX max_qp_rd_atom 16, spark4 ibv_devinfo). 더 띄우려면 연결 수(connections)를 늘린다. NIC 가 바뀌면 그 최대를 다시 본다"),
    K("ib-timeout", "RDMA", "ib_socket.timeout · retry_cnt", "packet 을 잃었을 때 다시 보내기까지 기다림 · 횟수", "FUSE 템플릿 · storage, 바로(새 연결부터인지 확인 필요)",
      ("14 (4.096 µs x 2^14 = 약 67 ms) · 7", "3FS IBSocket.h"), "todo",
      why="손실 스위치에서 끝 packet 을 잃으면 RC 가 timeout 뒤에 다시 보낸다. 줄이면 꼬리 지연이 줄 수 있지만 헛 재전송 위험이 있다. 기존 연결에 걸리는지 먼저 본다"),
    K("ib-qos", "RDMA", "traffic_class · sl · pkey", "RoCE 우선순위 class", "FUSE 템플릿 · storage",
      ("0", "3FS IBSocket.h · IBDevice.h"), "out", why="스위치의 PFC · ECN 이 꺼져 있어 class 가 할 일이 없다"),
    K("network", "RDMA", "RoCE active_mtu · 스위치 PFC · ECN", "망", "하드웨어 · 스위치 콘솔",
      ("—", "—"), "fixed", why="스위치 MTU 1500 이라 RoCE 1024, 스위치 PFC · ECN 끔, 호스트 NIC 는 global pause 켬(모든 트래픽 priority 0). 읽기 손실은 스위치가 incast 에서 버리고, 쓰기 손실은 storage NIC 가 수신에서 버린다(rx_discards_phy). 스위치 설정은 클러스터 관리자가 다룬다. 결과가 이 클러스터에 묶이는 가장 큰 까닭"),

    # --- layout (폴더마다, 클러스터 설정 아님) ---
    K("chunk", "layout", "chunk size (admin_cli set-layout)", "파일을 나누는 단위 = IO 하나 = RPC 에 담기는 최소 단위", "폴더마다",
      ("1 MiB", "3FS deploy/README.md init-cluster"), "sweep",
      cands=[("ch512k", "512 KiB"), ("c4m", "4 MiB"), ("c512k", "512 KiB · 읽기 32/8 · 묶음 512KB (세 값을 함께 바꿔 chunk 효과로 못 읽는다)")],
      when="크면 IO 하나가 커져 한 번에 몰리는 양이 크고 key 하나가 쓰는 chain 이 준다. 작으면 읽기 손실이 줄지만 store 의 IO 수가 늘어 store 가 먼저 막힌다 (Qwen 512 KiB: 버림 1/7, store 7.0 → 약 5.6 GiB/s). store 쪽 손잡이와 겹쳐 볼 후보"),
    K("stripe", "layout", "stripe size (admin_cli set-layout)", "파일 하나가 퍼지는 chain 수", "폴더마다",
      ("16 (upstream 예시) · 이 클러스터 8 (chain 8개)", "3FS deploy/README.md init-cluster"), "sweep",
      cands=[("stripe4", "4"), ("stripe1", "1"), ("stripe1-b1m", "1 · 묶음 1MB")]),
]

# 기준 상황 — 다시 돌리기로 확인한 기준 단계. 결과는 tune 폴더(ver+trace+model)와 screen 폴더(a 만)에서 끌어온다
SITUATIONS = [
    dict(id="coder x Qwen3-14B · hf3fs", short="Qwen", ver="a", trace="coder", model="qwen3-14b", knob="s16-d16", base=None, note="0.40: 낮 세 번 중 2, 같은 밤 두 번 중 0 — 경계"),
    dict(id="api(B) x Qwen3-14B · hf3fs", short="api(B)", ver="a", trace="apib", model="qwen3-14b", knob="s16-d16", base=0.25, note="0.25: 17번 중 8 — store 달성이 문턱(0.95) 위아래라 때에 따라 갈린다(밤 3/3, 10-23 낮 2/7). 기준 단계로 0.25 를 두고 후보는 같은 때 기준과 견준다"),
    dict(id="chat(A) x Qwen3-14B · hf3fs", short="chat(A)", ver="a", trace="chata", model="qwen3-14b", knob="s16-d16", base=0.20, note="한 번"),
    dict(id="coder x DeepSeek-V3 · hf3fs", short="DeepSeek", ver="a", trace="coder", model="ds-v3", knob="s16-d16", base=0.25, note="0.25 세 번 중 2"),
    dict(id="coder x Llama-70B · hf3fs", short="Llama", ver="a", trace="coder", model="llama-70b", knob="s16-d1", base=0.30, note="0.35 세 번 중 1"),
    dict(id="coder(60-63분) x Qwen3-14B · hf3fs", short="Qwen 60분", ver="a", trace="coder60", model="qwen3-14b", knob="s16-d16", base=None, note="10-23 주말 — 같은 trace 의 다른 구간"),
    dict(id="coder x Qwen3-14B · nixl_store_dynamic", short="dyn Qwen", ver="dyn", trace="coder", model="qwen3-14b", knob="dram_zc", base=None, note="한 번"),
    dict(id="coder x Qwen3-14B · nixl_native", short="nat Qwen", ver="nat", trace="coder", model="qwen3-14b", knob="w4", base=None, note="한 번"),
    dict(id="coder x Llama-70B · nixl_store_dynamic", short="dyn Llama", ver="dyn", trace="coder", model="llama-70b", knob="dram_zc", base=None, note="한 번"),
    dict(id="coder x DeepSeek-V3 · nixl_store_dynamic", short="dyn DeepSeek", ver="dyn", trace="coder", model="ds-v3", knob="dram_zc-a4k", base=None, note="한 번"),
    dict(id="coder x DeepSeek-V3 · nixl_native", short="nat DeepSeek", ver="nat", trace="coder", model="ds-v3", knob="w16-a4k", base=None, note="한 번"),
    dict(id="coder x Llama-70B · nixl_native", short="nat Llama", ver="nat", trace="coder", model="llama-70b", knob="w16-a4k", base=None, note="한 번"),
]
DOC_KNOB = {"a": "doc", "dyn": "auto", "nat": "w4"}      # adapter 문서 예시 설정


def wm(s):
    return ("" if s["trace"] == "coder" else f"-{s['trace']}") + ("" if s["model"] == "qwen3-14b" else f"-{s['model']}")


def runs(s):
    """상황 하나의 {tag: {단계: [통과 여부...]}} — 결과 폴더가 남아 있고 무효가 아닌 실행만"""
    out = {}
    dirs = [OUT / f"{s['ver']}{wm(s)}"] + ([OUT / f"a{wm(s)}-screen"] if s["ver"] == "a" else [])
    for d in dirs:
        if not (d / "result.json").exists():
            continue
        for tag, lv in json.loads((d / "result.json").read_text()).items():
            for l, m in lv.items():
                f = d / "runs" / f"{tag}-u{l}" / "summary.json"
                if m.get("valid") is False or not f.exists():
                    continue
                out.setdefault(tag, {}).setdefault(float(l), []).append(Run(bool(m["ok"]), f.stat().st_mtime))
    return out


class Run(int):
    """통과 여부(0/1, int 처럼 더한다)와 잰 때"""
    def __new__(cls, ok, t):
        x = super().__new__(cls, ok)
        x.t = t
        return x


WINDOW = 3 * 3600      # 같은 때 = 앞뒤 세 시간 (같은 설정도 시간에 따라 한 단계 흔들린다 — 10-21 · 10-22)


def merged(r, s, name):
    """후보 이름 하나의 모든 실행(tune 의 '설정+클러스터 설정', screen 의 이름 · 다시 돌리기)을 단계별로 합친다"""
    tags = {f"{s['knob']}+{name}", *(f"{s['knob']}+{name}.r{i}" for i in range(2, 10)), f"{name}+base", *(f"{name}+base.r{i}" for i in range(2, 10)),
            name, f"{name}.r2", f"{name}.r3", f"rep1-{name}", f"rep2-{name}"}
    if name in ("base", s["knob"]):            # 그 상황의 adapter 설정 = 기준
        tags = {f"{s['knob']}+base", *(f"{s['knob']}+base.r{i}" for i in range(2, 10)), "rep1-base", "rep2-base"}
    rep = re.compile(rf"rep\d+-{re.escape('base' if name in ('base', s['knob']) else name)}$")    # 다시 돌리기(rep1- · rep3- …)
    # tune.py 다시 돌리기(.r2 · .r21 …) — 번호 범위를 두지 않는다(10-23: .r10 위가 표에서 빠지던 것)
    pre = (f"{s['knob']}+base", ) if name in ("base", s["knob"]) else (f"{s['knob']}+{name}", f"{name}+base", name)
    rr = re.compile("^(" + "|".join(re.escape(p) for p in pre) + r")\.r\d+$")
    lv = {}
    for t in (tags | {x for x in r if rep.match(x) or rr.match(x)}) & set(r):
        for l, oks in r[t].items():
            lv.setdefault(l, []).extend(oks)
    return lv


def best(lv):
    """세 번 중 두 번처럼 절반 넘게 통과한 가장 높은 단계와 그 단계의 (통과, 횟수)"""
    ok = [l for l, oks in lv.items() if sum(oks) * 2 > len(oks)]
    if not ok:
        return None, None
    b = max(ok)
    return b, (sum(lv[b]), len(lv[b]))


def base_of(r, s):
    if s["base"]:
        return s["base"]
    return best(merged(r, s, "base"))[0]


def cell(r, s, name):
    lv, ref = merged(r, s, name), base_of(r, s)
    if name in ("src", "src-w32"):   # 소스 기본값은 문서 예시 설정에서 쟀다 — 같은 adapter 설정의 기준(REF)과 견준다
        dk = DOC_KNOB[s["ver"]]
        lv, ref = r.get(f"{dk}+{name}", {}), best(r.get(f"{dk}+base", {}))[0]
    if not lv:
        return ""
    if name == s["knob"] and s["base"] and s["base"] in lv:    # 기준 설정 자신 — 정해 둔 기준 단계의 통과 수를 낸다(자신과 견주지 않는다)
        oks = lv[s["base"]]
        return f"{s['base']:.2f} (기준) {sum(oks)}/{len(oks)}"
    b, pn = best(lv)
    if b is None:
        lo = min(lv)
        near = [x for x in merged(r, s, "base").get(lo, []) if any(abs(x.t - c.t) <= WINDOW for c in lv[lo])]
        if near and sum(near) * 2 <= len(near):     # 같은 때 잰 기준도 그 단계를 절반 넘게 통과하지 못했다 — 나쁘다고 할 수 없다
            return f"{lo:.2f} 미만 ? (같은 때 기준 {sum(near)}/{len(near)}, 후보 {sum(lv[lo])}/{len(lv[lo])})"
        return f"{lo:.2f} 미만"
    if ref is None:
        return f"{b:.2f}" + (f" {pn[0]}/{pn[1]}" if pn[1] > 1 else "")
    d = GRID.index(b) - GRID.index(ref)
    # 한 단계 차이를 가른 단계(나으면 b, 나쁘면 b 바로 위)에서 후보와 기준의 통과 비율을 견준다.
    # 기준도 경계 단계에서 세 번 중 두 번쯤 통과한다 — 비율 차이가 절반 미만이거나 세 번 못 돌렸으면 흔들림 안일 수 있다
    unsure = False
    if abs(d) == 1:
        dl = b if d > 0 else GRID[GRID.index(b) + 1]
        c_oks, b_oks = lv.get(dl, []), merged(r, s, "base").get(dl, [])
        near = [b for b in b_oks if any(abs(b.t - c.t) <= WINDOW for c in c_oks)]     # 같은 때 잰 기준이 있으면 그것과 견준다
        b_oks = near or b_oks
        rate = lambda x: sum(x) / len(x)
        unsure = len(c_oks) < 3 or (bool(b_oks) and abs(rate(c_oks) - rate(b_oks)) < 0.5)
    return f"{b:.2f} ({d:+d}{'?' if unsure else ''})" + (f" {pn[0]}/{pn[1]}" if pn[1] > 1 else "")


def render():
    R = {s["id"]: runs(s) for s in SITUATIONS}
    L = ["# 손잡이 목록 — LMCache L2 adapter x 3FS", "",
         "> `tune/knobs.py` 에서 낸 문서다. 고칠 때는 knobs.py 를 고치고 `python3 tune/knobs.py` 를 다시 돌린다. 측정 칸은 결과 폴더에서 끌어온다.", "",
         "모델이나 adapter 버전이 바뀔 때마다 가장 좋은 설정을 다시 찾아야 한다. 이 목록은 그때 **무엇을 후보로 넣고, 어디부터 보고, 결과를 어떻게 믿을지**를 정한다.",
         "목록은 기억이 아니라 소스의 설정 항목을 훑어 만들었다. IO 경로에 있는 항목은 도는 것 · 아직 안 잰 것 · 뺀 것 · 상황 입력 중 하나로 모두 들어 있다.", "",
         "- 측정은 이 클러스터의 것이다: Spark 클라이언트 3대, storage 4대(chain 8 · 복제 2 · target 16), RoCE MTU 1024, PFC · ECN 이 꺼진 손실 스위치",
         "- 손잡이마다 **언제 움직이나**(다른 서버에도 통하는 기구)와 **측정**(이 클러스터에서 얼마)을 나눠 적는다",
         "- 기준선은 소스 기본값과 문서 예시다. \"우리 값\"은 그 위에 이 서버에 맞춰 바꾼 것이다", "",
         "## 판정", "",
         "- 단계 = 디스크 읽기 천장(27.04 GiB/s) 대비 부하. 따라감 = 시작 늦음 p99 1초 미만 · store 달성 95% 이상 · 못 찾은 load 1% 미만",
         "- 한 번짜리 실행은 경계에서 한 단계씩 흔들린다. 기준 설정도 경계 단계에서 세 번 중 한두 번만 통과한다",
         "- 그래서 한 단계 차이는 그 단계를 두 번 더 돌려 세 번 중 두 번으로 정한다(`screen.py`). 두 단계 넘는 차이는 한 번으로 믿는다",
         "- **기준은 후보와 같은 때 다시 잰다.** 같은 설정도 클러스터의 배경 상태(3FS GC · 사용률)에 따라 한 단계 흔들린다(api(B) 0.25: 낮 1/3, 밤 3/3)",
         "- 경계에서는 통과 여부가 문턱 근처의 흔들림으로 갈린다. 한 단계 차이는 store 달성 · 못 찾음 같은 연속 값의 평균도 함께 본다",
         "- 측정 칸: `최대 단계 (기준 대비 단계 차이) 통과/횟수`. 통과/횟수는 그 단계를 여러 번 돌렸을 때만 적는다.",
         "  `?` 는 한 단계 차이가 흔들림 안일 수 있는 것 — 그 단계를 세 번 못 돌렸거나, 후보와 기준의 통과 비율 차이가 절반 미만이다",
         "  (기준도 경계 단계에서 세 번 중 두 번쯤 통과하므로 후보의 세 번 중 하나는 기준과 가를 수 없다). 기준 비율은 후보와 같은 때(앞뒤 세 시간)에 잰 기준이 있으면 그것으로 낸다.",
         "  `—` 는 그 상황에서 안 쟀다. 열 이름은 기준 상황 표에 있다", "",
         "## 새 모델 · 새 adapter 가 오면", "",
         "1. **상황 입력**을 정한다 — 모델의 token 당 KV 바이트(key = 256 token), 대상 서버의 GPU KV · L1 크기, trace. 아래 fixed 항목은 최적화할 값이 아니다",
         "2. **기준선**을 잰다 — 소스 기본값 + adapter 문서 예시",
         "3. **adapter 설정** 후보를 그 adapter 의 표에서 고른다 — 문서 예시 · 기본값 · 앞뒤 한 칸(절반 · 두 배)",
         "4. **클러스터 손잡이**는 key 크기로 먼저 볼 것을 고른다",
         "   - key 가 작다(약 20 MiB 아래): GiB 당 파일이 많다 → 폴더 수 · store worker",
         "   - key 가 크다(약 40 MiB 위): 한 번에 날아가는 읽기가 크다 → request-to-send 를 먼저 본다(가장 고르게 듣는다). 같은 벽의 묶음 · admission · 읽기 대상은 그 위에서 더 오르지 않았다(Llama)",
         "   - store 벽에 걸리는 trace(api(B) · chat(A)): store worker · storage store 쪽 RDMA · FUSE 쓰기 admission",
         "5. 한 번에 하나만 바꾼다(나머지는 측정으로 고른 값에 둔다). 전부 겹쳐 재는 것(n x n)은 잡음 때문에 칸마다 세 번씩이라 하지 않는다",
         "6. 겹침은 **다른 벽**을 상대하는 손잡이끼리만 — 같은 벽 손잡이끼리는 겹쳐도 오르지 않았다(Llama 의 읽기 쪽 셋: 하나씩 0.40, 둘을 겹쳐도 0.40).",
         "   벽: 읽기 몰림(읽기 admission · 묶음 · request-to-send · 읽기 대상) / store 처리량(쓰기 admission · storage RDMA · store worker · chunk) / 파일 만들기(폴더 · store worker) / adapter 경로(mem_config · 정렬)", "",
         "## 기준선 위에 쌓기 (coder x Qwen3-14B)", "",
         "| adapter | 기준선 (문서 예시 + FUSE 소스 기본값) | + FUSE 읽기 12/3 (측정으로 고른 값) | + adapter 설정 최적 | 그 위 클러스터 설정 최적 |", "|---|---|---|---|---|"]
    for s in SITUATIONS:
        if s["trace"] != "coder" or s["model"] != "qwen3-14b":
            continue
        r, dk = R[s["id"]], DOC_KNOB[s["ver"]]
        me = f"{s['knob']}+base"
        lvl = lambda t: best(merged(r, s, "base") if t == me else r[t])[0]          # 기준 설정은 다시 돌리기까지 합친다
        top = lambda tags: max(((lvl(t), t == me, t) for t in tags if lvl(t) is not None), default=(None, False, ""))   # 같으면 기준 설정
        ak = top([t for t in r if t.endswith("+base")])                                   # adapter 설정마다 기준(REF)에서
        f = lambda x: "" if x[0] is None else f"{x[0]:.2f} (`{x[2]}`)"
        # 그 위 클러스터 설정: 기준 상황의 adapter 설정에서 바꾼 것 중 가장 높은 것 — 측정 칸과 같은 표기(흔들림이면 ?)
        names = {t.split("+", 1)[1] for t in r if t.startswith(f"{s['knob']}+") and t.split("+", 1)[1].split(".")[0] not in ("base", "src")}
        cs = max(((best(merged(r, s, n))[0], n) for n in names if best(merged(r, s, n))[0] is not None), default=(None, ""))
        col = "" if cs[0] is None else (f"`{cs[1]}` {cell(r, s, cs[1])}" if ak[0] is not None and cs[0] > ak[0] else "오르지 않음")
        src = f"{dk}+src-w32" if f"{dk}+src-w32" in r else f"{dk}+src"      # 참 소스 기본값(쓰기도 32/8)이 있으면 그것
        L.append(f"| {s['id'].split(' · ')[1]} | {f(top([src]))} | {f(top([f'{dk}+base']))} | {f(ak)} | {col} |")
    L += ["", "## 기준 상황", "", "| 열 | 상황 | adapter 설정 | 기준 단계 | 근거 |", "|---|---|---|---|---|"]
    for s in SITUATIONS:
        b = base_of(R[s["id"]], s)
        L.append(f"| {s['short']} | {s['id']} | {s['knob']} | {'' if b is None else f'{b:.2f}'} | {s['note']} |")
    L += ["", "## 한눈에", "", "| 층 | 손잡이 | 상태 | 기준선 → 우리 값 |", "|---|---|---|---|"]
    for k in KNOBS:
        L.append(f"| {k['layer']} | `{k['key']}` | {k['tier']} | {k['base'][0]}" + (f" → {k['ours'].split(' (')[0].split(' — ')[0]}" if k["ours"] else "") + " |")
    layer = None
    for k in KNOBS:
        if k["layer"] != layer:
            layer = k["layer"]
            L += ["", f"## {layer}"]
        L += ["", f"### {k['id']} — {k['tier']}", "", f"- 무엇: {k['what']} (`{k['key']}`)", f"- 바꾸는 법: {k['how']}",
              f"- 기준선: {k['base'][0]} ({k['base'][1]})"]
        if k["ours"]:
            L.append(f"- 우리 값: {k['ours']}")
        for f, label in (("when", "언제 움직이나"), ("cost", "비용 · 위험"), ("why", "까닭")):
            if k[f]:
                L.append(f"- {label}: {k[f]}")
        rows = []
        for name, val in k["cands"]:
            cells = [cell(R[s["id"]], s, name) for s in SITUATIONS]
            rows.append((name, val, cells))
        used = [i for i, s in enumerate(SITUATIONS) if any(c[i] for _, _, c in rows)]
        if rows:
            L += ["", "| 후보 | 값 |" + "".join(f" {SITUATIONS[i]['short']} |" for i in used), "|---|---|" + "---|" * len(used)]
            for name, val, cells in rows:
                L.append(f"| `{name}` | {val} |" + "".join(f" {cells[i] or '—'} |" for i in used))
            if not used:
                L.append("")
                L.append("아직 안 쟀다.")
    return "\n".join(L) + "\n"


def check():
    """도구(tune.py · screen.py)의 후보와 이 목록이 같은지 본다. 도구에만 있는 후보가 있으면 실패한다"""
    sys.path.insert(0, str(HERE))
    import screen as SC
    import tune as T
    tool = ({n for n in T.SETTINGS if n != "base"} | {n for v in T.ADAPTERS.values() for n in v["knobs"]} | {c[0] for c in SC.CANDS})
    mine = {n for k in KNOBS for n, _ in k["cands"]}
    extra = sorted(tool - mine)
    if extra:
        raise SystemExit(f"목록에 없는 도구 후보: {extra} — knobs.py 에 먼저 올린다")
    return sorted(mine - tool)          # 목록에만 있는 것 = 도구가 아직 못 도는 후보


if __name__ == "__main__":
    todo = check()
    text = render()
    if "--stdout" in sys.argv:
        print(text)
    else:
        (HERE / "KNOBS.md").write_text(text)
        print(f"KNOBS.md {len(text.splitlines())} 줄. 도구가 아직 못 도는 후보: {todo}")
