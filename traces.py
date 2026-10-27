"""공개 request trace 목록과, trace 의 request 를 token 목록(prompt)으로 바꾸는 법.

trace 는 request 마다 도착 시각, 입력·출력 길이, prefix block 의 번호(`hash_ids`)를 준다
(Mooncake 512 token block, Alibaba 16 token block). prompt 내용은 없으므로 block 번호를 seed 로 한
난수로 block 크기만큼 token 을 만든다. 같은 번호는 언제나 같은 token 이 되어 trace 의 재사용이 그대로 살아난다.
"""

import json
import random
from pathlib import Path

HERE = Path(__file__).resolve().parent
GIB = 1024**3
TRACES = [  # 이름, 파일, block 크기(token), 시각 단위(초로 나눌 값)
    ("mooncake-conversation", "traces/mooncake/conversation_trace.jsonl", 512, 1000),
    ("mooncake-toolagent", "traces/mooncake/toolagent_trace.jsonl", 512, 1000),
    ("alibaba-chat(A)", "traces/alibaba/qwen_traceA_blksz_16.jsonl", 16, 1),
    ("alibaba-api(B)", "traces/alibaba/qwen_traceB_blksz_16.jsonl", 16, 1),
    ("alibaba-thinking", "traces/alibaba/qwen_thinking_blksz_16.jsonl", 16, 1),
    ("alibaba-coder", "traces/alibaba/qwen_coder_blksz_16.jsonl", 16, 1),
    # 데워진 구간 검증용으로 줄인 coder (results/hot-20261019/make_hot.py 가 만든다)
    ("alibaba-coder-hot30", "traces/alibaba/coder_hot30.jsonl", 16, 1),
]


def prompt_tokens(r: dict, block: int) -> list[int]:
    """request 의 token 목록. 같은 block 번호 → 같은 token(block 크기만큼)."""
    toks = []
    for h in r["hash_ids"]:
        rnd = random.Random(h)
        toks += [rnd.randrange(1000, 150000) for _ in range(block)]
    return toks[:r["input_length"]]


def load(trace: str) -> tuple[list[dict], int, float]:
    """trace 의 request 들(도착 순), block 크기, 시각 단위."""
    name, rel, block, tdiv = next(t for t in TRACES if t[0] == trace)
    reqs = sorted((json.loads(l) for l in open(HERE / rel)), key=lambda r: r["timestamp"])
    return reqs, block, tdiv


def head(trace: str, max_requests: int | None = None, max_len: int | None = None,
         minutes: float | None = None) -> tuple[list[dict], int]:
    """trace 앞에서부터(처음 minutes 분 안에서) max_len 이하인 request 를 max_requests 개까지 (검증용)."""
    reqs, block, tdiv = load(trace)
    t0 = reqs[0]["timestamp"] / tdiv
    out = [r for r in reqs if (max_len is None or r["input_length"] <= max_len)
           and (minutes is None or r["timestamp"] / tdiv - t0 < minutes * 60)]
    return out[:max_requests], block
