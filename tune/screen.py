#!/usr/bin/env python3
"""설정 후보 거르기 — 기준 상황(hf3fs x coder x Qwen3-14B, store 16 · 폴더 16, FUSE 기본값)에서 아직 안 잰 후보를
IO 경로 층마다(동시성 · 크기 · 배치) 하나씩 바꿔, 결과를 바꾸는 것만 남긴다 (10-20 사용자 요청, 09:00 전에 끝내고 되돌린다).

  거르기: 기준의 최대 단계 L 에서 한 번. 통과면 L 위 한 단계 — 통과하면(기준보다 낫다) 계속 올려 최대를 찾는다.
          L 에서 실패면(기준보다 나쁘다) 한 단계 아래 한 번
  후보에서 뺀 것(코드로 확인): hf3fs io_depth · ior_entries · iov_size — worker 하나가 key 를 IO 한 번(prep · submit · wait 1)으로 읽고 쓰며,
          key(40-80 MiB)가 iov(200 MB) 안에 든다. FUSE 쓰기 admission — CONFIG_ITEM 이라 FUSE 를 다시 띄워야 한다(10-19)
  storage RDMA 동시 수: 사용자 허락(10-20 "크게 문제 없으면 껴서"). hot-update, 메모리에서만, 끝에 256 으로 되돌리고 네 대 확인

    screen.py [마감 HH:MM, 기본 08:30] [--model M] [--folders 16|1] [--cands a,b] [--repeat 0.40,0.45 --times 2]
      --model · --folders: 다른 상황(그 모델의 matrix 결과에서 가장 좋았던 adapter 설정)에서 거른다
      --repeat: 거르기 대신 기준 설정을 그 단계들에서 --times 번 더 돌린다(같은 설정의 흔들림)
"""
import argparse
import json
import os
import shutil
import signal
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import tune as T                      # noqa: E402
from replay import admin, ensure_layout   # noqa: E402

S, GRID = T.S, T.GRID
RUN_S = 8 * 60                        # 실행 하나의 넉넉한 길이(미리 쓰기 포함)
ST_DEFAULT = dict(S.ST_HOME)            # 되돌릴 곳(cluster [home] st_rdma). 소스 기본값은 256 · 256
# storage 의 RDMA 동시 수(storage NIC 마다): writes = storage 가 클라이언트에 써 주는 쪽(load 응답), reads = 당겨 오는 쪽(store)
CANDS = [
    ("st-load32", dict(st=dict(writes=32))),
    ("st-store32", dict(st=dict(reads=32))),
    ("stripe4", dict(stripe=4)),
    ("stripe1", dict(stripe=1)),
    ("rw32", dict(workers=32)),
    ("rw8", dict(workers=8)),
    ("st-load8", dict(st=dict(writes=8))),
    ("stripe1-b1m", dict(stripe=1, fuse=dict(batch="1MB"))),     # 10-21: Llama-70B 에서 둘 다 나았다 — 겹치나
    ("b1m", dict(fuse=dict(batch="1MB"))),                        # 다시 돌리기용 (matrix 의 b1m 과 같은 설정)
    ("d32", dict(nd=32)), ("d64", dict(nd=64)),                    # 10-21: DeepSeek-V3 는 폴더 1 → 16 이 2.9배였다 — 더 늘리면
    # 10-21 밤: 손잡이 목록을 채울 후보. FUSE 읽기 — 동시에 떠 있는 바이트(32/8 · 1MB = 32 MB)와 request-to-send(w41 의 손실 없는 설정)
    ("r32-1m", dict(fuse=dict(read="32/8", batch="1MB"))),
    ("rts", dict(fuse=dict(rdma="true", rts="on"))),
    ("r32-1m-rts", dict(fuse=dict(read="32/8", batch="1MB", rdma="true", rts="on"))),
    ("st-store512", dict(st=dict(reads=512))),                    # store 쪽 32 가 나빴다 — 기본 256 위로
    ("s8", dict(store=8)), ("r8", dict(retrieve=8)), ("s32", dict(store=32)),   # rw8 · rw32 를 store · retrieve 로 나눈다
    ("lk8", dict(lookup=8)),
    ("st512-r32-1m", dict(st=dict(reads=512), fuse=dict(read="32/8", batch="1MB"))),   # 10-21 밤: api(B) 에서 둘 다 +1 — 겹치나
    ("rts-b1m", dict(fuse=dict(batch="1MB", rdma="true", rts="on"))),   # 10-22: Llama 에서 둘 다 +2 — 겹치나
    ("w32", dict(fuse=dict(write="32/8"))),   # FUSE 쓰기 admission 소스 기본값 — FUSE 재시작 뒤 FUSE_WRITE=32/8 로만 돈다
    ("w128", dict(fuse=dict(write="128/64"))),   # 64/32 가 32/8 보다 1-2 단계 넘게 나았다 — 더 키우면. FUSE_WRITE=128/64 로만 돈다
    # 10-22: storage 의 읽기 요청 coroutine(소스 기본 64/8, 쓰기 쪽 update 만 쟀다) · 복제 중 어디서 읽나(기본 LoadBalance)
    ("cr128", dict(cr="128/16")), ("cr256", dict(cr="256/32")),
    ("ts-rr", dict(fuse=dict(tsel="RoundRobin"))), ("ts-rand", dict(fuse=dict(tsel="RandomTarget"))), ("ts-head", dict(fuse=dict(tsel="HeadTarget"))),
    # 10-22 밤: storage load 쪽 RDMA 를 늘리면(안 잰 칸) · 양쪽 / Llama 에서 둘 다 +1-2 인 request-to-send 와 Random 읽기 대상(같은 벽 — 겹치지 않을 것으로 본다)
    ("st-load512", dict(st=dict(writes=512))), ("st-both512", dict(st=dict(writes=512, reads=512))),
    ("rts-rand", dict(fuse=dict(rdma="true", rts="on", tsel="RandomTarget"))),
    # 10-23: store 벽은 storage NIC 수신 버림과 함께 나타난다 — store 데이터를 당겨 오는 동시 수를 32(나빴다)와 256 사이로
    ("st-store64", dict(st=dict(reads=64))), ("st-store128", dict(st=dict(reads=128))),
    ("wb1m", dict(fuse=dict(wbatch="1MB"))),
    # 10-23: 연결 하나의 동시 RDMA READ 는 16(하드웨어 최대). storage 가 store 를 당겨 오는 것도 그 연결 위라, 쌍마다 연결을 늘리면 그 상한이 오른다
    ("conn2", dict(fuse=dict(conn="2"))), ("conn4", dict(fuse=dict(conn="4"))),
    # 10-23 주말: request-to-send 를 켠 뒤로도 읽기 admission 12/3 이 맞나(12/3 은 끈 채 골랐다) · 허락 수가 다시 묶나(admission 을 올리면)
    ("rts-r8", dict(fuse=dict(read="8/2", rdma="true", rts="on"))), ("rts-r16", dict(fuse=dict(read="16/4", rdma="true", rts="on"))),
    ("rts-r24", dict(fuse=dict(read="24/6", rdma="true", rts="on"))), ("r32-rts", dict(fuse=dict(read="32/8", rdma="true", rts="on"))),
    ("r32-rts-mct16", dict(fuse=dict(read="32/8", rdma="true", rts="on", mct="16"))),
    ("rts-r6", dict(fuse=dict(read="6/2", rdma="true", rts="on"))),   # 10-24: rts 위에서 8/2 가 12/3 보다 조금 나았다 — 더 낮추면   # FUSE 쓰기 묶음 4MB → 1MB(CONFIG_ITEM) — fuse_write.py 64/32 1MB 뒤 FUSE_WBATCH=1MB 로만 돈다
]


def knob(v, nd=16):
    w = v.get("workers", 16)
    ops = '{"lookup":%d,"retrieve":%d,"store":%d}' % (v.get("lookup", 2), v.get("retrieve", w), v.get("store", w))
    aj = T.hf3fs(nd, True).replace('{"lookup":2,"retrieve":16,"store":16}', ops)
    return dict(nd=nd, aj=aj, py=T.HF_PY, prefix=T.AT)


def data_dir(stripe, nd=16, wm=""):
    d = f"{T.DATA}/screen{wm}-s{stripe}"
    for x in [d] + ([f"{d}/p{i}" for i in range(nd)] if nd > 1 else []):
        ensure_layout(x, stripe, 1048576)
    lay = {l.split()[0]: l.split()[-1] for l in admin(f"stat --display-layout {T.C.rel(d)}").splitlines()
           if l.startswith("Layout-")}
    if lay.get("Layout-StripeSize") != str(stripe):
        raise SystemExit(f"{d} stripe {lay.get('Layout-StripeSize')} (원한 것 {stripe})")
    return d


set_st, set_cr, CR_DEFAULT = S.set_st, S.set_cr, S.CR_HOME     # storage RDMA 동시 수 · 읽기 coroutine(settings.py)


class Screen(T.Sweep):
    def __init__(self, deadline, model="qwen3-14b", folders=16, trace="coder"):
        super().__init__("a", trace, model)
        self.ref = json.loads(self.res_path.read_text()).get(f"s16-d{folders}+base", {}) if self.res_path.exists() else {}
        self.out = T.OUT / f"a{self.wm}-screen"
        (self.out / "runs").mkdir(parents=True, exist_ok=True)
        self.res_path = self.out / "result.json"
        self.res = json.loads(self.res_path.read_text()) if self.res_path.exists() else {}
        self.deadline = deadline

    def measure(self, tag, c, d, level):
        if time.time() + RUN_S > self.deadline:
            raise TimeoutError
        return super().measure(tag, c, d, level)

    def ok3(self, tag, c, d, lv, first):
        """lv 에서 두 번 더 돌린 세 번 중 통과 수. 한 번짜리는 경계에서 한 단계씩 흔들린다(10-21 다시 돌리기)"""
        return int(first["ok"]) + sum(int(self.measure(f"{tag}.r{k}", c, d, lv)["ok"]) for k in (2, 3))

    def screen(self, tag, c, d, base_lv):
        """base_lv 에서 한 번. 한 단계 차이는 그 단계를 두 번 더 돌려 세 번 중 두 번으로 정한다. 두 단계 넘는 차이는 한 번으로 믿는다"""
        i = GRID.index(base_lv)
        m = self.measure(tag, c, d, GRID[i])
        if m["ok"]:
            best, top = GRID[i], m
            while i + 1 < len(GRID):
                i += 1
                m = self.measure(tag, c, d, GRID[i])
                if not m["ok"]:
                    break
                best, top = GRID[i], m
            if best == base_lv:
                verdict = "기준과 같다"
            elif GRID.index(best) - GRID.index(base_lv) == 1:
                n = self.ok3(tag, c, d, best, top)
                verdict = f"낫다 ({base_lv:.2f} → {best:.2f}, 세 번 중 {n})" if n >= 2 else f"기준과 같다 ({best:.2f} 세 번 중 {n} — 흔들림)"
                best = best if n >= 2 else base_lv
            else:
                verdict = f"낫다 ({base_lv:.2f} → {best:.2f})"
        else:
            n = self.ok3(tag, c, d, base_lv, m)
            if n >= 2:
                best, verdict = base_lv, f"기준과 같다 ({base_lv:.2f} 세 번 중 {n})"
            else:
                m = self.measure(tag, c, d, GRID[i - 1])
                best = GRID[i - 1] if m["ok"] else None
                verdict = f"나쁘다 ({base_lv:.2f} 세 번 중 {n}" + (f", {best:.2f} 통과)" if best else f", {GRID[i - 1]:.2f} 도 실패)")
        S.log(f"   {tag} 거르기: {verdict}")
        return best, verdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("hm", nargs="?", default="08:30")
    ap.add_argument("--model", default="qwen3-14b", choices=list(T.MODELS))
    ap.add_argument("--folders", type=int, default=16, choices=[1, 16])
    ap.add_argument("--trace", default="coder", choices=list(T.WORKLOADS))
    ap.add_argument("--cands", help="후보 이름 (기본: 전부)")
    ap.add_argument("--repeat", help="기준 설정을 다시 돌릴 단계들")
    ap.add_argument("--times", type=int, default=2)
    ap.add_argument("--rep-start", type=int, default=1, help="다시 돌리기 tag 의 첫 번호(rep{N}-) — 앞서 쓴 번호와 겹치지 않게")
    ap.add_argument("--base-lv", type=float, help="기준의 최대 단계(다시 돌리기로 확인한 값). 기본: tune 결과의 최대")
    args = ap.parse_args()
    hm, nd = args.hm, args.folders
    now = time.localtime()
    dl = time.mktime(time.strptime(time.strftime("%Y-%m-%d ", now) + hm, "%Y-%m-%d %H:%M"))
    if dl < time.time():
        dl += 86400
    for s in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(s, lambda *_: sys.exit(1))
    sc = Screen(dl, args.model, nd, args.trace)
    ok = [float(k) for k, m in sc.ref.items() if m["ok"]]
    base_lv = args.base_lv or (max(ok) if ok else 0.35)
    # --cands 에 base 를 넣으면 기준 설정도 같은 호출에서 돈다(같은 때 · 미리 쓴 chunk 를 같이 쓴다, 10-23 주말)
    cands = [c for c in [("base", {})] + CANDS if (args.cands and c[0] in args.cands.split(",")) or (not args.cands and c[0] != "base")]
    S.log(f"== {'다시 돌리기' if args.repeat else '거르기'} ({args.model}, store 16 · 폴더 {nd}, 기준 최대 단계 {base_lv:.2f}, 마감 {hm})")
    summary = {}
    bad = S.check_home()
    if bad:
        raise SystemExit("클러스터가 되돌릴 곳에 있지 않다 — 되돌리면 지금 값을 지운다. 확인하고 맞춘 뒤 다시: " + "; ".join(bad))
    try:
        S.apply({**S.BASE})
        if T.C.allowed("storage_hot_update"):
            set_st(**ST_DEFAULT)
        if args.repeat:                                      # --cands 를 주면 그 후보를, 아니면 base 를 다시 돌린다
            for name, v in (cands if args.cands else [("base", {})]):
                set_st(**{**ST_DEFAULT, **v.get("st", {})})
                set_cr(v.get("cr", CR_DEFAULT))
                S.apply({**S.BASE, **v.get("fuse", {})})
                for lv in (float(x) for x in args.repeat.split(",")):
                    for k in range(args.rep_start, args.rep_start + args.times):
                        m = sc.measure(f"rep{k}-{name}", knob(v, v.get("nd", nd)), data_dir(v.get("stripe", 8), v.get("nd", nd), sc.wm), lv)
                        summary.setdefault(f"{name} {lv:.2f}", []).append(m["ok"])
            S.log(f"   다시 돌리기: {summary}")
        for name, v in ([] if args.repeat else cands):
            try:
                if "st" in v or "cr" in v:
                    set_st(**{**ST_DEFAULT, **v.get("st", {})})
                    set_cr(v.get("cr", CR_DEFAULT))
                S.apply({**S.BASE, **v.get("fuse", {})})
            except S.NotAllowed as e:
                S.log(f"   {name}: {e} 을 바꿀 허락이 없어 건너뛴다")
                continue
            best, verdict = sc.screen(name, knob(v, v.get("nd", nd)), data_dir(v.get("stripe", 8), v.get("nd", nd), sc.wm), base_lv)
            summary[name] = {"best": best, "verdict": verdict}
            if "st" in v or "cr" in v:
                set_st(**ST_DEFAULT)
                set_cr(CR_DEFAULT)
            S.apply({**S.BASE})
    except TimeoutError:
        S.log("마감 — 남은 후보는 하지 않는다")
    finally:
        try:
            S.restore()
        finally:
            shutil.rmtree(T.DATA, ignore_errors=True)
            S.log(f"재생 폴더 지움: {not os.path.exists(T.DATA)}")
            (sc.out / ("repeat.json" if args.repeat else "screen.json")).write_text(json.dumps(summary, ensure_ascii=False, indent=1))
            S.log("끝")


if __name__ == "__main__":
    main()
