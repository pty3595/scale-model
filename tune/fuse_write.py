#!/usr/bin/env python3
"""FUSE 쓰기 admission 을 바꾸고 클라이언트의 FUSE 를 다시 띄운다 (10-22 사용자 허락: 쓰기 32/8 측정). 클러스터 [allow] fuse_restart 가 있어야 한다.
쓰기 admission 은 CONFIG_ITEM 이라 템플릿만 올려서는 안 바뀐다.

  fuse_write.py 32/8    템플릿에 쓰기 32/8 을 올리고 다시 띄운다(읽기 등 나머지는 되돌릴 템플릿 그대로)
  fuse_write.py 64/32   되돌릴 템플릿(cluster threefs.fuse_template, 이 클러스터는 64/32)으로 되돌리고 다시 띄운다
  fuse_write.py 64/32 1MB   쓰기 묶음(max_batch_bytes, 이것도 CONFIG_ITEM)도 함께 바꾼다(10-23). 기본은 템플릿 값

다른 사람의 서빙이 돌면 하지 않는다. 다시 띄운 뒤 마운트 · 쓰기 한도를 확인하고, 하나라도 다르면 실패로 끝낸다.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tune as T                      # noqa: E402

S, C = T.S, T.C


def main():
    C.require("fuse_restart", "FUSE 다시 띄우기")
    want = sys.argv[1]
    wbatch = sys.argv[2] if len(sys.argv) > 2 else S.HOME_TPL["wbatch"]
    why = T.busy()
    if why:
        raise SystemExit("다른 작업이 보여 하지 않는다: " + "; ".join(why))
    c = dict(S.HOME, write=want, wbatch=wbatch)
    S.log(f"템플릿에 쓰기 {want} · 묶음 {wbatch} 을 올리고 클라이언트 FUSE 를 다시 띄운다")
    S.log(f"마운트 ok · 쓰기 {want} · 묶음 {wbatch}\n    {S.push_fuse(c)}")     # 다시 띄우고 실제 값까지 본다(settings.restart_fuse)


if __name__ == "__main__":
    main()
