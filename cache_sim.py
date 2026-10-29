#!/usr/bin/env python3
"""LMCache MP 모드의 캐시 시뮬레이터. GPU 계산은 하지 않고, 요청마다 청크(256토큰)가
어디서 오고 무엇이 3FS(L2)로 저장되는지만 계산한다.

층은 셋이다.
  GPU  vLLM prefix cache. 16토큰 블록, 인스턴스(= LMCache MP 서버 하나)마다 하나
  L1   LMCache MP 서버의 CPU 메모리. 청크 단위
  L2   3FS. 모든 인스턴스가 공유

규칙 (LMCache 0.28.0 a44d5e5 · vLLM 0.28.0 코드와 Spark 실험으로 확인한 것)
  GPU  - 앞에서부터 이어지는 블록만 적중. 전체가 맞아도 마지막 토큰은 다시 계산하므로 적중은 (L-1)//16 블록까지
       - 새 블록은 free 큐 앞(가장 오래 안 쓴 것)에서 꺼내고, 끝난 요청의 블록은 뒤 블록부터 free 큐 뒤에 붙인다
         (그래서 요청의 앞쪽 블록이 가장 늦게 밀려난다)
  L1   - lookup 은 청크 0부터 이어지는 L1 적중(p1)을 센 뒤, 나머지를 L2 에 묻는다. L2 도 앞에서부터 이어지는 만큼(p2)
       - L2 적중 범위(p2) 중 L1 에 남아 있는 청크는 L1 에서 가져오고 나머지만 L2 에서 읽는다. GPU 에 이미 있어도 읽는다
         (요청이 들어올 때 prefetch). 읽은 청크는 임시라 L1 에 남지 않는다 (PrefetchPolicy 기본값: 전부 임시).
         GPU 적중 안이라 vLLM 이 가져가지 않은 청크도 남지 않는다 (thinking 실험으로 확인)
       - 요청이 끝나면 프롬프트 청크 중 L1 에 있는 것을 뒤에서부터 touch → 청크 0 이 가장 최근.
         단, 마지막 prefill 단계의 저장 청크는 touch 뒤에 목록에 들어가므로 그것들이 가장 최근이 된다
       - 메모리 사용률이 수위(0.8) 이상이면 LRU 목록 키 개수의 비율(0.2)만큼 목록 앞에서 지운다. 비우기는 1초 주기라
         대개 요청 시작 직후(prefill 도중)에 일어나고, 그때 prefetch 임시 객체와 처음 두 prefill 단계의 저장 청크는
         메모리는 차지하지만 LRU 목록에는 없다(그래서 지울 개수에서 빠진다). 그 요청의 L1 적중 청크는 GPU 로 옮긴 뒤라
         잠겨 있지 않고 touch 전이라 밀려날 수 있다. L2 적중 범위 안의 L1 청크는 L2 읽기가 끝날 때까지 잠겨 있고,
         3FS 저장이 진행 중인 청크도 잠겨 있다
  저장 - prefill 은 max_num_batched_tokens 씩 단계로 나뉘고 단계마다 저장한다. 전체 범위는 LMCache 적중 끝(p1+p2 청크)부터
         "계산된 토큰"까지의 완전한 청크.
         계산된 토큰에는 출력 토큰도 들어간다(출력 max_tokens 면 max_tokens−1 개). 출력으로 채워진 청크의 key 는
         생성 내용을 모르므로 세지 않는다(프롬프트 청크에만 라벨을 붙인다)
       - LMCache 적중이 0 이면 vLLM 적중 토큰 수가 기록되지 않아(get_num_new_matched_tokens 가 먼저 반환),
         계산된 토큰 = L − vLLM 적중 이 된다 → GPU 가 청크보다 짧게 적중하면 뒤쪽 청크가 저장되지 않는다
       - 저장할 청크는 L1 에 쓰고(뒤에서부터 created → 앞 청크가 최근), L2 adapter 에 보낸다.
         adapter 는 이미 가진 key 를 건너뛴다(중복 제출). adapter 용량(max_capacity_gb, GiB)을 넘으면 그 저장 작업의
         나머지를 건너뛰고, L2 비우기가 없으므로 이후 저장은 모두 건너뛴다 (thinking 전체 실행: 6,801번째 요청부터)

LMCache 판에 따라 바뀐 규칙 (lmcache.rules, 없으면 a44d5e5) — 8d7d2c4(10-01) 실제 실행 대조(results/hot-20261019)로 확인.
  prefetch controller v2(#5346, 09-25)부터의 동작이라 8eb36fa · ecb031a 빌드도 같은 코드다(lookup 쪽만 코드로 확인)
  8d7d2c4  - L2 lookup 은 request 의 청크 전부를 묻는다(prefetch_controller: "full keys"). 적중은 L1 · L2 어느 쪽에든 있는 청크가
             청크 0 부터 이어지는 만큼이고, 청크마다 L1 에 있으면 L1, 없으면 L2 에서 가져온다(DefaultPrefetchPolicy)
           - LMCache 적중이 0 이어도 vLLM 적중 토큰이 계산된 토큰에 들어간다 → 마지막 완전한 청크까지 저장한다
  ad1d738  8d7d2c4 에 더해 60538a0(10-17, #5383): GPU(vLLM APC)가 가진 앞부분 c0 = GPU 적중 // 청크 는 묻지도 읽지도 않고
             L1 · L2 recency 만 갱신한다(touch, IO 없음). 적중은 c0 부터 L1 · L2 어느 쪽에든 이어지는 만큼이고, 서버가 c0 을 더해
             절대값으로 알린다 → 저장도 c0 + 적중 끝에서 시작해, L2 에 없는 GPU 앞부분은 다시 저장하지 않는다(8d7d2c4 는 적중이
             0 부터라 그런 청크를 저장했다). 라벨 gpu = GPU 가 가진 앞부분(3FS IO 없음)

요청 하나의 결과는 청크마다 라벨 하나:
  L1     L1 적중 (L2 에 묻지 않음)
  L2     L2 에서 load
  store  새로 계산해 처음으로 저장을 보냄 (adapter 용량을 넘었으면 실제로는 저장되지 않는다 — skipped 로 센다)
  dup    새로 계산해 저장을 보냈지만 전에 보낸 적이 있음 (L2 에 있으면 adapter 가 건너뛴다)
  lost   3FS IO 없음 — 계산했지만 저장되지 않았거나, L1 앞부분이 끊긴 뒤라 L2 에 물었지만 L1 에 있어서 읽지 않음
"""

import collections
import math

LABELS = ("L1", "L2", "store", "dup", "lost", "gpu")


class CacheSim:
    def __init__(self, cfg: dict, gpu_blocks: int, chunk_key=None):
        """cfg: targets/serving-base.json 모양. gpu_blocks: vLLM 이 잡은 KV 블록 수.
        chunk_key: 토큰 목록 → 청크 key 목록 함수 (없으면 파이썬 해시)."""
        lm, v = cfg["lmcache"], cfg["vllm"]
        if lm.get("mode") != "mp":      # 규칙(1초 비우기 루프, MP connector 동작)이 MP 서버의 것이다
            raise ValueError(f"LMCache MP 모드만 시뮬레이션한다 (설정의 lmcache.mode = {lm.get('mode')!r})")
        self.chunk, self.block = lm["chunk_size"], v["block_size"]
        self.step_tokens = v["max_num_batched_tokens"]      # chunked prefill 한 단계
        align = lm.get("l1_align_bytes", 4096)
        obj = math.ceil(self.chunk * cfg["model"]["kv_bytes_per_token"] / align) * align
        self.l1_cap = lm["l1_size_gb"] * 2**30 / obj          # L1 에 들어가는 청크 수
        self.wm, self.ratio = lm["eviction_trigger_watermark"], lm["eviction_ratio"]
        self.free = collections.OrderedDict((b, None) for b in range(gpu_blocks))   # 앞 = 먼저 꺼냄
        self.bhash, self.h2b = {}, {}
        self.l1 = collections.OrderedDict()                   # 앞 = LRU
        self.l2 = set()                                       # 3FS 에 실제로 있는 청크 (인스턴스끼리 공유)
        self.submitted = set()                                # 저장을 보낸 적 있는 청크 (라벨용, 공유)
        cap = cfg.get("l2", {}).get("max_capacity_gb")       # adapter 용량 상한(GiB). 넘으면 저장을 건너뛴다
        self.l2_cap = cap * 2**30 / (self.chunk * cfg["model"]["kv_bytes_per_token"]) if cap else math.inf
        self.l2_mine = 0                                      # 이 인스턴스의 adapter 가 저장한 청크 수
        self.skipped = 0
        self.chunk_key = chunk_key or self._py_chunk_keys
        self.evictions = 0
        # L2 쓰기 승인(실험용, LMCache 에는 없다): "second" 면 이 인스턴스가 처음 계산한 chunk 는 L1 에만 두고,
        # 다시 계산하게 될 때 L2 에 쓴다. seen 을 인스턴스끼리 공유하면 클러스터 단위 승인이 된다
        self.admit = lm.get("l2_admit")
        self.seen = set()
        self.rules = lm.get("rules", "a44d5e5")              # LMCache 판별 규칙 (머리말)

    # --- key ---
    def _py_chunk_keys(self, toks):
        out, h = [], None
        for i in range(0, len(toks) - len(toks) % self.chunk, self.chunk):
            h = hash((h, tuple(toks[i:i + self.chunk])))
            out.append(h)
        return out

    def _block_keys(self, toks):
        out, h = [], None
        for i in range(0, len(toks) - len(toks) % self.block, self.block):
            h = hash((h, tuple(toks[i:i + self.block])))
            out.append(h)
        return out

    # --- L1 ---
    def _evict_check(self, extra_mem=0, extra_order=0, locked=()):
        """비우기는 요청 도중에 일어난다. 사용률은 메모리 기준(extra_mem: 아직 LRU 목록에 없는 객체 포함),
        지울 개수는 LRU 목록 기준(extra_order: 목록에 있는 임시 객체)이고, 그 요청이 쓰는 청크(locked)는 건너뛴다."""
        if (len(self.l1) + extra_mem) / self.l1_cap < self.wm:
            return
        k = max(1, int((len(self.l1) + extra_order) * self.ratio))
        victims = []
        for key in self.l1:
            if key not in locked:
                victims.append(key)
                if len(victims) >= k:
                    break
        for key in victims:
            del self.l1[key]
        self.evictions += 1

    def _l1_created(self, keys):
        for k in reversed(keys):
            self.l1[k] = None
            self.l1.move_to_end(k)

    def _l1_touch(self, keys):
        for k in reversed(keys):
            if k in self.l1:
                self.l1.move_to_end(k)

    # --- 요청 하나 ---
    def request(self, toks, max_tokens=1, ck=None, timing=None):
        """timing(선택, 검증용): {"s": 시작, "e": 응답, "next": 다음 요청 시작, "tick": x 이후 첫 비우기 검사 시각 함수}.
        주면 비우기가 prefill 도중(기본)인지, 응답 뒤인지, 다음 요청으로 넘어가는지를 시각으로 가린다."""
        L = len(toks)
        ck = ck if ck is not None else self.chunk_key(toks)
        bk = self._block_keys(toks)
        n = len(ck)

        # GPU prefix cache
        hit = 0
        while hit < len(bk) and bk[hit] in self.h2b:
            hit += 1
        hit = min(hit, (L - 1) // self.block)
        h = hit * self.block

        # LMCache lookup: (ad1d738 은 GPU 가 가진 앞부분 s0 를 건너뛰고) L1 prefix → L2 prefix
        s0 = h // self.chunk if self.rules == "ad1d738" else 0
        if s0:
            self._l1_touch(ck[:s0])                           # 그 앞부분은 lookup 때 recency 만 갱신한다
        p1 = 0
        while s0 + p1 < n and ck[s0 + p1] in self.l1:
            p1 += 1
        p2 = 0
        if self.rules == "a44d5e5":                           # L1 앞부분 뒤를 L2 에 묻는다
            while s0 + p1 + p2 < n and ck[s0 + p1 + p2] in self.l2:
                p2 += 1
        else:                                                 # 청크 전부를 묻고 L1 · L2 를 합쳐 앞에서부터
            while s0 + p1 + p2 < n and (ck[s0 + p1 + p2] in self.l2 or ck[s0 + p1 + p2] in self.l1):
                p2 += 1
        ret = (s0 + p1 + p2) * self.chunk                     # 서버는 적중을 절대값(s0 포함)으로 알린다
        # L2 적중 범위 중 L1 에 남아 있는 청크는 L1 에서 가져오고, 나머지만 L2 에서 읽는다.
        # 8d7d2c4 까지는 GPU 적중과 무관하게 읽는다 (요청이 들어올 때 prefetch 한다)
        from_l2 = [ck[j] not in self.l1 for j in range(s0 + p1, s0 + p1 + p2)]
        load = sum(from_l2)

        # GPU 블록: 적중 블록은 사용 중으로, 나머지는 free 큐 앞에서 꺼낸다
        in_use = [self.h2b[bk[i]] for i in range(hit)]
        for b in in_use:
            self.free.pop(b, None)
        need = math.ceil((L + max_tokens - 1) / self.block) - hit
        for _ in range(need):
            b, _ = self.free.popitem(last=False)
            old = self.bhash.pop(b, None)
            if old is not None and self.h2b.get(old) == b:
                del self.h2b[old]
            in_use.append(b)
        for i in range(hit, len(bk)):                        # 프롬프트의 완전한 블록은 캐시된다
            b = in_use[i]
            self.bhash[b] = bk[i]
            self.h2b[bk[i]] = b

        # 저장 — prefill 은 step_tokens 씩 여러 단계로 나뉘고, 단계마다 그때까지 계산된 완전한 청크를 저장한다.
        # 계산된 토큰 = 스케줄된 새 토큰 + max(추적된 vLLM 적중, LMCache 적중). LMCache 적중이 0 이면 vLLM 적중이
        # 추적되지 않는다(0). 출력 max_tokens 면 출력 max_tokens−1 개도 한 토큰씩 더해진다
        start = (L - 1) if ret >= L else max(h, ret)         # vLLM 이 새로 계산하기 시작하는 위치
        base = max(h if ret > 0 or self.rules != "a44d5e5" else 0, ret)
        steps, left = [], L - start
        while left > 0:
            steps.append(min(self.step_tokens, left))
            left -= steps[-1]
        steps += [1] * (max_tokens - 1)
        batches, end, sched = [], s0 + p1 + p2, 0
        for st in steps:
            sched += st
            new_end = min(n, (sched + base) // self.chunk)
            if new_end > end:
                batches.append(ck[end:new_end])
                end = new_end
        labels = ["gpu"] * s0 + ["L1"] * p1 + ["L2" if x else "lost" for x in from_l2]
        stored = [k for b in batches for k in b]
        for b in batches:                                    # 단계마다 저장 작업 하나. adapter 는 이미 가진 key 를 건너뛰고,
            full = False                                     # 용량을 넘으면 그 작업의 나머지를 건너뛴다
            for k in b:
                if self.admit == "second" and k not in self.seen and k not in self.submitted:
                    self.seen.add(k)                         # 처음 계산: L2 IO 없음
                    labels.append("lost")
                    continue
                labels.append("dup" if k in self.submitted else "store")
                self.submitted.add(k)
                if k in self.l2:
                    continue
                if full or self.l2_mine + 1 > self.l2_cap:
                    full = True
                    self.skipped += 1
                    continue
                self.l2.add(k)
                self.l2_mine += 1
        labels += ["lost"] * (n - len(labels))
        # 비우기는 1초 주기라 대개 요청 시작 후 0.1~1.2초, prefill 이 끝나기 전에 일어난다. 그때 L1 메모리에는
        # prefetch 한 임시 객체와 처음 두 단계의 저장 청크가 잡혀 있지만 아직 LRU 목록에는 없다
        # (thinking·coder 실험: 목록 밖 객체 = 임시 + min(저장, 2단계). prefill 이 비우기보다 먼저 끝난 짧은 요청은
        #  저장이 이미 목록에 들어가 있었다 — 시점에 달린 것이라 시뮬레이터에 넣지 않았다)
        inflight = load + sum(k not in self.l1 for b in batches[:2] for k in b)
        mode = "now"
        if timing and (len(self.l1) + inflight) / self.l1_cap >= self.wm:
            t = timing["tick"](timing["s"])
            mode = "next" if t >= timing["next"] else ("after" if t >= timing["e"] else "now")
        if mode == "now":
            # L1 적중 청크는 GPU 로 옮긴 직후 잠금이 풀리고 touch 는 요청 끝이라, 이 요청이 쓰는 청크도 밀려날 수 있다
            # (thinking 전체 실행: 요청 282 가 L1 적중으로 쓴 청크가 282 도중 비우기에서 빠졌다)
            # 반면 L2 적중 범위 안에서 L1 에 있던 청크는 L2 읽기가 끝날 때까지 함께 잠겨 있다
            # (thinking 전체 실행: 요청 973 이 L1 에서 가져온 청크 46~79 는 도중 비우기에서 살아남았다)
            held = {ck[j] for j, x in zip(range(s0 + p1, s0 + p1 + p2), from_l2) if not x} if load else set()
            self._evict_check(extra_mem=inflight, locked=held)
        # 앞 단계의 저장 청크는 요청 도중 목록에 들어가고, 요청이 끝날 때 touch 가 일어난 뒤
        # 마지막 단계의 저장 청크가 들어간다 (coder 실험: 마지막 단계 청크가 LRU 에서 가장 최근)
        for b in batches[:-1]:
            self._l1_created(b)

        # 끝: GPU 블록 반납(뒤 블록부터), L1 touch, 마지막 단계 저장
        for b in reversed(in_use):
            self.free[b] = None
        self._l1_touch(ck)
        if batches:
            self._l1_created(batches[-1])
        if mode == "after":                                  # 응답 뒤, 3FS 저장이 끝나기 전: 저장 청크는 목록에 있고 잠겨 있다
            self._evict_check(locked=set(stored))
        # mode == "next": 다음 요청이 시작할 때 그 요청의 검사에서 비운다
        return {"labels": labels, "gpu_hit_tokens": h, "p1": p1, "p2": p2, "s0": s0, "loaded": load, "store_batches": batches,
                "lookup_from": p1 if self.rules == "a44d5e5" else s0}        # L2 lookup 이 묻는 첫 청크
