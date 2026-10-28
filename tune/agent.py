#!/usr/bin/env python3
"""AI 에이전트의 입구 — RUNBOOK.md 의 점검 · 측정 계획 · 지켜보기 · 멈추기 · 결과 보고를 명령 하나씩으로 한다.
명령 모양이 늘 같아 권한 허락을 `python3 tune/agent.py` 한 줄로 줄 수 있고, 측정은 점검을 건너뛰지 못한다.
무엇을 어떤 순서로 잴지는 PLAN 이 정한다 — 앞 단계 결과로 다음 단계의 knob · 클러스터 설정을 고른다.

  agent.py install      클라이언트마다 adapter 셋 · NIXL HF3FS plugin · pwrite shim 이 있고 빌드가 같은지. 실패면 종료 코드 1
  agent.py check        측정 전 점검. 하나라도 실패면 종료 코드 1
  agent.py plan         측정 계획의 단계마다 끝났나 · 남았나 · 걸리는 시간
  agent.py next         점검을 통과하면 계획에서 아직 안 끝난 첫 단계를 띄운다(터미널이 끊겨도 돈다)
  agent.py status       돌고 있는 측정, 끝난 실행 줄(버림 포함), 끝났는지
  agent.py stop         agent.py 가 띄운 측정만 멈추고 HOME 으로 돌아왔는지 본다
  agent.py report       결과 보고 표를 낸다(results/tune-speed/report-<시각>.md 에도 쓴다)
  agent.py coverage [--trace T] [--model M]   그 trace x 모델에서 knob · 클러스터 설정마다 잰 것 · 안 잰 것
  agent.py start ADAPTER --knobs a,b --trace T --model M (--settings x,y | --start S --repeat N --rep-start K)
                        사용자가 계획 밖의 것을 콕 집어 시켰을 때만. 인자는 tune.py sweep 그대로
"""
import json
import os
import re
import signal
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
SM = HERE.parent
sys.path[:0] = [str(HERE), str(SM)]
import cluster as C               # noqa: E402
import settings as S              # noqa: E402
import tune as T                  # noqa: E402
from guard import busy            # noqa: E402

STATE = T.OUT / "agent.json"      # agent.py 가 띄운 측정(pid · 로그 · 단계)
DISK_BUSY = 100                   # storage 노드 하나의 NVMe 읽기+쓰기 MiB/s 가 이보다 크면 누가 3FS 를 쓰고 있다
RUN_LINE = re.compile(r" x[0-9.]+ +load ")
NOT_RUN = {"raw_hf3fs": "RUNBOOK: raw_hf3fs 는 돌리지 않는다(3FS 소스 빌드가 필요하고, 그 PR 의 test 는 클러스터의 모든 chunk 를 지운다)"}
AD = ["hf3fs", "nixl_store_dynamic", "nixl_native"]   # 잴 adapter 와 순서 — hf3fs 가 기준

# --- 측정 계획 (Spark 에서 정했다, 10-28) ---
# 1 기준 상황(coder x Qwen3-14B, 클러스터 base)에서 adapter 셋의 knob 전부 — adapter 끼리 견주는 것이 이 도구의 목적이라 셋 다 잰다
# 2 adapter 마다 가장 좋은 knob 에서 클러스터 설정(허락된 것) — tune.py 의 기본 후보
# 3 adapter 마다 가장 좋은 조합의 최대 배속과 그 위 한 칸을 두 번씩 더(세 번 중 두 번으로 경계를 정한다)
# 4 다른 trace · 모델에서 adapter 마다 1 의 가장 좋은 knob(클러스터 base). Spark 에서 모델마다 가장 좋은 knob 이 달랐다 — 시간 때문에 하나만
BASE_WL = ("coder", "qwen3-14b")
SETS = ["b1m", "r32-1m", "r6", "rts", "c512k"]
OTHER = [("coder", "ds-v3"), ("coder", "llama-70b"), ("apib", "qwen3-14b"), ("chata", "qwen3-14b"), ("coder60", "qwen3-14b")]


def folder(adapter, trace, model):
    """tune.py Sweep 의 결과 폴더(같은 규칙)"""
    n = T.ADAPTERS[adapter].get("clients")
    return T.OUT / (adapter + ("" if trace == "coder" else f"-{trace}") + ("" if model == "qwen3-14b" else f"-{model}")
                    + ("" if not n or n == len(C.CLIENTS) else f"-n{n}"))


def results(a, tr, mo):
    f = folder(a, tr, mo) / "result.json"
    return json.loads(f.read_text()) if f.exists() else {}


def ok(m):
    return m["ok"] and m.get("valid", True)


def speeds(res, tag, reps=True):
    """tag(reps 면 그 .rN 도)의 배속 → 실행들"""
    by = {}
    for t, sp in res.items():
        if t == tag or (reps and t.startswith(tag + ".r")):
            for s, m in sp.items():
                by.setdefault(float(s), []).append(m)
    return by


def up_of(s):
    i = T.GRID.index(T.on_grid(s))
    return T.GRID[i + 1] if i + 1 < len(T.GRID) else None


def best_speed(res, tag):
    p = [s for s, ms in speeds(res, tag, False).items() if any(ok(m) for m in ms)]
    return max(p) if p else None


def search_done(res, tag):
    """search 가 끝났나 — 통과한 최대의 위 한 칸을 쟀거나, 통과 없이 가장 낮은 칸까지 내려갔다"""
    by = speeds(res, tag, False)
    b = best_speed(res, tag)
    return bool(by) and ((up_of(b) in by or up_of(b) is None) if b else T.GRID[0] in by)


def allowed_setting(name):
    c = {**S.BASE, **{k: v for k, v in T.SETTINGS[name].items() if k != "chunk"}}
    return not ((not C.allowed("fuse_template") and any(c[k] != S.HOME[k] for k in S.FUSE_KEYS))
                or (not C.allowed("storage_hot_update") and any(c[k] != S.HOME[k] for k in S.ST_KEYS)))


def best_tag(res, tags):
    """끝난 search 중 최대 배속이 가장 높은 tag (같으면 앞의 것)"""
    done = [(best_speed(res, t) or 0, -i, t) for i, t in enumerate(tags) if search_done(res, t)]
    return max(done)[2] if done else None


def steps():
    """계획의 단계: (이름, adapter, trace, model, tune.py 인자 | None(앞 단계를 기다림), 끝났나, 설명)"""
    out = []
    tr, mo = BASE_WL
    bk = {}
    for a in AD:
        res, ks = results(a, tr, mo), list(T.ADAPTERS[a]["knobs"])
        left = [k for k in ks if not search_done(res, f"{k}+base")]
        out.append(("1 knob 전부", a, tr, mo, [a, "--knobs", ",".join(ks), "--trace", tr, "--model", mo, "--settings", "base"],
                    not left, f"knob {len(ks) - len(left)}/{len(ks)}"))
        bk[a] = None if left else best_tag(res, [f"{k}+base" for k in ks]).split("+")[0]
    for a in AD:
        res, k = results(a, tr, mo), bk[a]
        sets = [s for s in SETS if allowed_setting(s)]
        skip = [s for s in SETS if s not in sets]
        left = [s for s in sets if k and not search_done(res, f"{k}+{s}")]
        note = (f"knob {k}, 설정 {len(sets) - len(left)}/{len(sets)}" if k else "1 단계를 기다린다") + (f", 허락 없어 뺌: {','.join(skip)}" if skip else "")
        out.append(("2 클러스터 설정", a, tr, mo,
                    [a, "--knobs", k, "--trace", tr, "--model", mo, "--settings", ",".join(["base"] + sets)] if k else None,
                    bool(k) and not left, note))
    for a in AD:
        res, k = results(a, tr, mo), bk[a]
        ready = k and all(search_done(res, f"{k}+{s}") for s in SETS if allowed_setting(s))
        combo = best_tag(res, [f"{k}+base"] + [f"{k}+{s}" for s in SETS if allowed_setting(s)]) if ready else None
        b = best_speed(res, combo) if combo else None
        for which, sp in (("최대", b), ("위 한 칸", up_of(b) if b else None)):
            n = [m for m in speeds(res, combo).get(sp, [])] if sp else []
            done = bool(combo) and (b is None or len(n) >= 3)
            args = ([a, "--knobs", k, "--trace", tr, "--model", mo, "--settings", combo.split("+")[1],
                     "--start", f"{sp:g}", "--repeat", "2", "--rep-start", "2"] if combo and sp else None)
            out.append((f"3 경계 확인({which})", a, tr, mo, args, done,
                        (f"{combo} x{sp:g} {len(n)}번 잼" if sp else ("통과한 배속 없음 — 건너뜀" if combo else "2 단계를 기다린다"))))
    for tr2, mo2 in OTHER:
        for a in AD:
            k = bk[a]
            res = results(a, tr2, mo2)
            out.append(("4 다른 trace · 모델", a, tr2, mo2,
                        [a, "--knobs", k, "--trace", tr2, "--model", mo2, "--settings", "base"] if k else None,
                        bool(k) and search_done(res, f"{k}+base"), f"knob {k}" if k else "1 단계를 기다린다"))
    return out


def plan():
    st = steps()
    left = [s for s in st if not s[5]]
    for name, a, tr, mo, args, done, note in st:
        print(f"[{'끝' if done else '남음' if args else '기다림'}] {name:16} {a:19} {tr:8} {mo:10} {note}")
    print(f"→ 남은 단계 {len(left)}/{len(st)}" + (" — 계획이 다 끝났다. agent.py report 로 보고한다" if not left else
          ". 한 단계가 search 1개(1 · 2 단계는 knob · 설정 수만큼)이고 search 하나가 1-2시간이다"))
    return left


# --- 설치 확인 ---
NEED = {   # adapter 가 import 하는 것 — 하나라도 없으면 그 adapter 는 못 돈다
    "hf3fs": ["lmcache.lmcache_native", "lmcache.lmcache_hf3fs", "lmcache.v1.distributed.l2_adapters.hf3fs_l2_adapter"],
    "nixl_store_dynamic": ["nixl._api", "lmcache.v1.distributed.l2_adapters.nixl_store_dynamic_l2_adapter"],
    "nixl_native": ["nixl._api", "lmcache.lmcache_native", "lmcache.lmcache_nixl", "lmcache.v1.distributed.l2_adapters.nixl_native_l2_adapter"],
}
PROBE = r'''
import hashlib, importlib, json, os, sys
need, plugin = json.loads(sys.argv[1]), sys.argv[2]
out = {}
for m in need:
    try:
        importlib.import_module(m)
        out[m] = "ok"
    except Exception as e:
        out[m] = f"없음 — {type(e).__name__}: {e}"[:240]
if plugin:
    try:
        import importlib.metadata, nixl        # nixl 은 torch 의 CUDA 빌드로 cu12 · cu13 backend 를 고른다. HF3FS plugin 은 cu13 에만 있다
        b = getattr(getattr(nixl, "_pkg", None), "__name__", "?")
        v = importlib.metadata.version("nixl")
        out["NIXL backend"] = f"ok {b} {v}" if b == "nixl_cu13" else f"없음 — {b} {v} 를 골랐다(torch CUDA {getattr(sys.modules.get('torch'), 'version', None) and sys.modules['torch'].version.cuda}). HF3FS plugin 은 nixl_cu13 에만 있다"
    except Exception as e:
        out["NIXL backend"] = f"없음 — {type(e).__name__}: {e}"[:240]
    p = os.path.join(plugin.replace("{venv}", sys.prefix), "libplugin_HF3FS.so")
    out["NIXL HF3FS plugin"] = "ok" if os.path.isfile(p) else f"없음 — {p}"
try:
    import lmcache
    pkg, tree = os.path.dirname(lmcache.__file__), hashlib.sha256()
    for f in sorted(os.path.join(d, n) for d, _, ns in os.walk(pkg) for n in ns):     # driver.py 의 지문과 같은 규칙
        r = os.path.relpath(f, pkg)
        if f.endswith((".py", ".so")) and "__pycache__" not in r.split(os.sep) and "tests" not in r.split(os.sep):
            tree.update(r.encode() + open(f, "rb").read())
    out["lmcache"] = f"{pkg} 지문 {tree.hexdigest()[:12]}"
except Exception as e:
    out["lmcache"] = f"없음 — {e}"
print(json.dumps(out, ensure_ascii=False))
'''


def install():
    """클라이언트마다 adapter 셋이 import 되는지, NIXL HF3FS plugin · pwrite shim 이 있는지, 빌드 지문이 같은지"""
    plugin = C.calibration()["adapter"]["env"]["NIXL_PLUGIN_DIR"]
    good, prints = True, {}
    for c in C.CLIENTS:
        a_ = C.addr(c["host"])
        for a in AD:
            py, pypath = T.build(a)
            cmd = f"cd {c['workdir']} && {pypath} {py or c['python']} - '{json.dumps(NEED[a])}' '{plugin if a.startswith('nixl') else ''}'"
            r = subprocess.run(C.remote(a_, cmd), input=PROBE, capture_output=True, text=True)
            try:
                res = json.loads(r.stdout.strip().splitlines()[-1])
            except (IndexError, json.JSONDecodeError):
                res = {"python": f"못 돌렸다 — {(r.stderr or r.stdout).strip()[-240:]}"}
            bad = {k: v for k, v in res.items() if k != "lmcache" and not v.startswith("ok")}
            good &= not bad
            prints.setdefault(a, {})[c["host"]] = res.get("lmcache", "")
            info = res.get("lmcache", "") + (f", {res['NIXL backend'][3:]}" if "NIXL backend" in res and not bad else "")
            print(f"[{'통과' if not bad else '실패'}] {c['host']:12} {a:19} " + ("; ".join(f"{k}: {v}" for k, v in bad.items()) or info))
        shim = C.sh(a_, f"test -f {T.PW_SHIM} && echo y", check=False).strip() == "y"
        good &= shim
        print(f"[{'통과' if shim else '실패'}] {c['host']:12} pwrite shim {T.PW_SHIM}" + ("" if shim else " 없음 — RUNBOOK 설치 4"))
    for a, ps in prints.items():
        same = len({p.split("지문 ")[-1] for p in ps.values()}) == 1
        good &= same
        print(f"[{'통과' if same else '실패'}] {a} 빌드 지문이 클라이언트끼리 " + ("같다" if same else "다르다 — 한 곳에서 빌드한 것을 같은 경로로 복사한다(RUNBOOK 설치 3)"))
    print("→ 설치 확인 통과 — adapter 셋 모두 돈다" if good else "→ 설치 확인 실패 — 실패한 줄을 RUNBOOK 의 설치 절대로 고치고 다시 확인한다. 고칠 수 없으면 그대로 보고한다")
    return good


# --- 점검 · 띄우기 ---
def sweeps():
    """이 장비에서 도는 tune.py sweep (pid, 명령)"""
    out = []
    for p in Path("/proc").iterdir():
        if p.name.isdigit():
            try:
                c = (p / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace").strip()
            except OSError:
                continue
            if "tune/tune.py sweep" in c and "agent.py" not in c:
                out.append((int(p.name), c))
    return out


def disk_rates():
    """storage 노드마다 NVMe 읽기+쓰기 MiB/s (3초 평균)"""
    def one(s):
        o = C.sh(C.addr(s["host"]), "cat /proc/diskstats; echo ===; sleep 3; cat /proc/diskstats")
        devs = {Path(d).name for d in s["nvme"]}
        a, b = ({f[2]: int(f[5]) + int(f[9]) for f in (l.split() for l in part.splitlines()) if len(f) > 9 and f[2] in devs}
                for part in o.split("==="))
        return s["host"], sum(b[d] - a.get(d, b[d]) for d in b) * 512 / 2**20 / 3
    with ThreadPoolExecutor(len(C.STORAGE)) as ex:
        return dict(ex.map(one, C.STORAGE))


def check():
    good = True

    def item(passed, what, detail):
        nonlocal good
        good &= passed
        print(f"[{'통과' if passed else '실패'}] {what}: {detail}")

    me = os.environ.get("USER", "")
    others = sorted({l.split()[0] for l in subprocess.run(["who"], capture_output=True, text=True).stdout.splitlines() if l.split()[0] != me})
    print(f"[정보] 나 말고 로그인한 계정: {', '.join(others) or '없음'} — 보고에 적는다(이것만으로는 멈추지 않는다)")
    why = busy()
    item(not why, "다른 사람의 작업(vLLM · LMCache 서버 · SGLang · Mooncake · Ray · GPU)", "; ".join(why) or "없음")
    r = disk_rates()
    item(all(v <= DISK_BUSY for v in r.values()), f"storage 디스크(노드마다 {DISK_BUSY} MiB/s 이하)",
         "  ".join(f"{h} {v:.0f}" for h, v in r.items()))
    run = sweeps()
    item(not run, "돌고 있는 측정", "; ".join(f"pid {p}: {c}" for p, c in run) or "없음")
    if not run:                   # 측정이 도는 동안은 일부러 HOME 이 아니다
        bad = S.check_home()
        item(not bad, "클러스터 설정이 HOME", "; ".join(bad) or "HOME 과 같다")
    fs = os.statvfs(C.MOUNT)
    item(fs.f_bavail / fs.f_blocks >= T.MIN_FREE, f"3FS 여유({T.MIN_FREE:.0%} 이상)", f"{fs.f_bavail / fs.f_blocks:.1%}")
    print("→ 점검 통과" if good else "→ 점검 실패 — 측정하지 않는다. 실패한 줄을 사용자에게 그대로 보고하고 기다린다")
    return good


def opt(args, name):
    return args[args.index(name) + 1] if name in args and args.index(name) + 1 < len(args) else None


def launch(args, what):
    if not check():
        raise SystemExit(1)
    a, tr, mo = args[0], opt(args, "--trace"), opt(args, "--model")
    log = T.OUT / f"{time.strftime('%m%d-%H%M')}-{a}-{tr}-{mo}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    with open(log, "w") as lf:
        p = subprocess.Popen([sys.executable, "tune/tune.py", "sweep", *args], cwd=SM, stdout=lf, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, start_new_session=True)
    STATE.write_text(json.dumps({"pid": p.pid, "log": str(log), "args": args, "what": what, "started": time.strftime("%m-%d %H:%M")},
                                ensure_ascii=False))
    print(f"띄웠다: {what}\n  pid {p.pid}, 로그 {log.relative_to(SM)}\n  tune.py sweep {' '.join(args)}")
    print("10-20분마다 `python3 tune/agent.py status` 로 본다. 끝나면(→ 끝났다) `agent.py next` 로 다음 단계를 띄운다")


def next_step():
    for name, a, tr, mo, args, done, note in steps():
        if not done:
            if args is None:
                raise SystemExit(f"{name} {a} 는 앞 단계를 기다린다 — agent.py plan 출력을 사용자에게 보고한다")
            return launch(args, f"{name} · {a} · {tr} x {mo} · {note}")
    print("계획이 다 끝났다 — `python3 tune/agent.py report` 로 보고한다")


def start(args):
    if not args or args[0] not in T.ADAPTERS:
        raise SystemExit(f"첫 인자는 adapter: {', '.join(AD)}")
    if args[0] in NOT_RUN:
        raise SystemExit(NOT_RUN[args[0]])
    knobs, trace, model, sets = (opt(args, k) for k in ("--knobs", "--trace", "--model", "--settings"))
    miss = [k for k, v in (("--knobs", knobs), ("--trace", trace), ("--model", model)) if not v]
    if not sets and "--repeat" not in args:
        miss.append("--settings (또는 --start · --repeat)")
    if miss:
        raise SystemExit(f"빠진 인자: {', '.join(miss)}")
    bad = ([f"knob {k}" for k in knobs.split(",") if k not in T.ADAPTERS[args[0]]["knobs"]]
           + [f"설정 {s}" for s in (sets or "").split(",") if s and s not in T.SETTINGS]
           + ([f"trace {trace}"] if trace not in T.WORKLOADS else []) + ([f"모델 {model}"] if model not in T.MODELS else []))
    if bad:
        raise SystemExit(f"없는 이름: {', '.join(bad)} — knob 은 agent.py coverage, trace · 모델은 RUNBOOK 의 표")
    launch(args, "사용자 지시")


def alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return "tune.py" in Path(f"/proc/{pid}/cmdline").read_bytes().decode(errors="replace")


def status():
    st = json.loads(STATE.read_text()) if STATE.exists() else None
    run = sweeps()
    print("돌고 있는 측정: " + ("; ".join(f"pid {p} {c}" for p, c in run) if run else "없음"))
    if not st:
        print("agent.py 로 띄운 측정이 아직 없다")
        return
    print(f"agent.py 가 띄운 것: {st['started']} {st.get('what', '')}\n로그: {Path(st['log']).relative_to(SM)}")
    lines = Path(st["log"]).read_text(errors="replace").splitlines()
    done = [l for l in lines if RUN_LINE.search(l) or " 최대 " in l]
    print(f"끝난 실행 {sum(1 for l in done if RUN_LINE.search(l))}개 — 실행 하나(5-20분)가 끝나야 그 줄이 나온다. 버림 · 디스크 · 판정이 그 줄에 있다:")
    for l in done[-15:]:
        print("  " + l.strip())
    if alive(st["pid"]):
        print("→ 돌고 있다")
    elif any("재생 폴더 지움" in l for l in lines):
        print("→ 끝났다. 클러스터: " + ("; ".join(S.check_home()) or "HOME 과 같다"))
    else:
        print("→ 끝나지 않고 멈췄다. 로그 끝 20줄:")
        for l in lines[-20:]:
            print("  " + l)


def stop():
    st = json.loads(STATE.read_text()) if STATE.exists() else None
    if not st or not alive(st["pid"]):
        print("agent.py 로 띄운 측정이 돌고 있지 않다 — 다른 측정은 멈추지 않는다")
        return
    os.killpg(os.getpgid(st["pid"]), signal.SIGTERM)
    print(f"pid {st['pid']} 에 멈추라고 했다. 도구가 클러스터를 되돌리기를 기다린다(몇 분)")
    t0 = time.time()
    while alive(st["pid"]) and time.time() - t0 < 900:
        time.sleep(2)
    if alive(st["pid"]):
        raise SystemExit("15분 안에 멈추지 않았다 — 사용자에게 보고한다")
    print("멈췄다. 클러스터: " + ("; ".join(S.check_home()) or "HOME 과 같다"))


# --- 결과 ---
def why_fail(m):
    return ("request" if (m.get("late_p99") or 0) >= T.A.LATE_MAX else "") or \
           ("store" if (m.get("store_ratio") or 0) < T.A.STORE_MIN else "") or ("못 찾음" if (m.get("missed_frac") or 0) >= T.A.MISS_MAX else "무효")


def cell(res, tag):
    """search tag(+ .rN)의 최대 배속 · 통과/횟수 · 위 한 칸 — 사람이 읽는 한 칸"""
    by = speeds(res, tag)
    if not by:
        return None
    inval = sum(1 for ms in by.values() for m in ms if not m.get("valid", True))
    passed = [s for s, ms in by.items() if any(ok(m) for m in ms)]
    if not passed:
        low = min(by)
        return f"통과 없음 (가장 낮은 x{low:g} 도 {why_fail(by[low][0])})" + (f", 무효 {inval}" if inval else "")
    b = max(passed)
    u = up_of(b)
    s = f"x{b:g} {sum(map(ok, by[b]))}/{len(by[b])}"
    if u in by:
        s += f" · 위 x{u:g} {sum(map(ok, by[u]))}/{len(by[u])} " + ",".join(sorted({why_fail(m) for m in by[u] if not ok(m)}) or {"-"})
    elif u:
        s += f" · 위 x{u:g} 안 잼"
    if not search_done(res, tag):
        s += "  (search 가 끝나지 않았다)"
    elif len(by[b]) < 3 or (u in by and len(by[u]) < 3):
        s += "  (경계 확인 전)"
    return s + (f", 무효 {inval}" if inval else "")


def coverage(trace, model):
    total = done = 0
    print(f"== {trace} x {model}  (search = adapter · knob · 클러스터 설정 하나의 최대 배속)")
    for a in AD:
        res = results(a, trace, model)
        print(f"-- {a}   ({(folder(a, trace, model) / 'result.json').relative_to(SM)}{'' if res else ', 아직 없음'})")
        for k in T.ADAPTERS[a]["knobs"]:
            c = cell(res, f"{k}+base")
            total, done = total + 1, done + search_done(res, f"{k}+base")
            print(f"   {k + '+base':26} {c or '-  안 잼'}")
        for t in sorted({t.split(".r")[0] for t in res if not t.split(".r")[0].endswith("+base")}):
            print(f"   {t:26} {cell(res, t)}")
    print(f"→ base 에서 search 가 끝난 knob {done}/{total}" + (" — 다 쟀다" if done == total else " — 안 잰 것이 있다. '다 했다'고 하지 않는다"))


def report():
    st = steps()
    left = [s for s in st if not s[5]]
    out = [f"## {time.strftime('%Y-%m-%d')} {C.NAME} 측정 결과", "",
           f"계획 단계 {len(st) - len(left)}/{len(st)} 끝" + (" — 계획이 다 끝났다" if not left else f" — 남은 단계 {len(left)}개(agent.py plan). 다 끝났다고 쓰지 않는다"),
           f"클러스터 설정 허락: FUSE 템플릿 {C.allowed('fuse_template')} · storage hot-update {C.allowed('storage_hot_update')}", ""]
    for tr, mo in [BASE_WL] + OTHER:
        rows = []
        for a in AD:
            res = results(a, tr, mo)
            tags = sorted({t.split(".r")[0] for t in res}, key=lambda t: -(best_speed(res, t) or 0))
            tags = [t for t in tags if search_done(res, t)]
            if not tags:
                rows.append(f"| {a} | - | 안 잼 | | | | |")
                continue
            t = tags[0]
            b = best_speed(res, t)
            if b is None:
                rows.append(f"| {a} | {t} | {cell(res, t)} | | | | |")
                continue
            m = next(x for x in speeds(res, t)[b] if ok(x))
            f3 = lambda x: "-" if x is None else f"{x:.2f}"
            run = folder(a, tr, mo) / "runs" / f"{t}-x{b:g}"
            drops = T.A.metrics(run).get("storage_rx_discards") if (run / "summary.json").exists() else None
            rows.append(f"| {a} | {t} | {cell(res, t)} | {f3(m.get('load'))} · {f3(m.get('store'))} | "
                        f"{f3(m.get('of_3fs_load'))} · {f3(m.get('of_3fs_store'))} | {'-' if drops is None else f'{drops:,}'} | "
                        f"{sum(1 for k in T.ADAPTERS[a]['knobs'] if search_done(res, f'{k}+base'))}/{len(T.ADAPTERS[a]['knobs'])} |")
        out += [f"### {tr} x {mo}", "",
                "| adapter | 가장 좋은 knob+설정 | 최대 배속 통과/횟수 · 위 한 칸 통과/횟수 막힌 쪽 | 최대에서 load · store GiB/s | 3FS 천장 대비 load · store | storage 수신 버림 | base 에서 잰 knob |",
                "|---|---|---|---|---|---|---|"] + rows + [""]
    out += ["- 추정: (에이전트가 채운다. 없으면 \"없음\")", "- 무효 · 멈춘 실행: (에이전트가 채운다. 없으면 \"없음\")",
            "- 클러스터: " + ("; ".join(S.check_home()) or "HOME 과 같다") if not sweeps() else "- 클러스터: 측정이 도는 중"]
    text = "\n".join(out)
    f = T.OUT / f"report-{time.strftime('%m%d-%H%M')}.md"
    f.write_text(text + "\n")
    print(text + f"\n\n(→ {f.relative_to(SM)})")


def main():
    a = sys.argv[1:]
    cmd = a[0] if a else ""
    if cmd == "install":
        sys.exit(0 if install() else 1)
    elif cmd == "check":
        sys.exit(0 if check() else 1)
    elif cmd == "plan":
        plan()
    elif cmd == "next":
        next_step()
    elif cmd == "start":
        start(a[1:])
    elif cmd == "status":
        status()
    elif cmd == "stop":
        stop()
    elif cmd == "report":
        report()
    elif cmd == "coverage":
        coverage(opt(a, "--trace") or BASE_WL[0], opt(a, "--model") or BASE_WL[1])
    else:
        print(__doc__)


if __name__ == "__main__":
    main()
