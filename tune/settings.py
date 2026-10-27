#!/usr/bin/env python3
"""클러스터 층 설정(FUSE 템플릿 · storage hot-update)을 걸고, 실제로 걸렸는지 보고, 되돌린다. 클러스터 값과 허락은 cluster.py 에서.

설정 하나 = dict. FUSE 템플릿: read(읽기 admission) batch(읽기 묶음) rdma(enable_rdma_control) write(쓰기 admission) wbatch(쓰기 묶음)
                              conn(연결 수) mct(request-to-send 동시 허락 수) tsel(복제 중 어디서 읽나)
                storage:      rts(request-to-send on|off) coro(쓰기 coroutine pool)
FUSE 는 클라이언트마다 mgmtd 설정 버전이 올라갔는지와 FUSE 로그에 찍힌 값으로, storage 는 노드마다 get-config 로 확인한다.
HOME(되돌릴 곳) = 그 클러스터의 FUSE 템플릿 사본의 값 + [home]. BASE(실험 기준) = [ref]. 템플릿은 되돌릴 곳일 뿐 실험의 기준이 아니다(10-21 사용자).
허락([allow])이 없는 쪽은 기준도 지금 값(HOME)으로 두고, 그 쪽을 바꾸는 후보는 NotAllowed 로 건너뛴다.
쓰기 둘은 CONFIG_ITEM 이라 FUSE 를 다시 띄워야 바뀐다. 다시 띄운 동안은 FUSE_WRITE · FUSE_WBATCH 로 지금 값을 알려 준다(fuse_write.py).

    settings.py state      지금 FUSE(클라이언트마다) · storage(노드마다) 값
    settings.py restore    HOME 으로 되돌리고 확인
"""
import os
import re
import sys
import time
import tomllib
from pathlib import Path

SM = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SM))
import cluster as C           # noqa: E402

TMP = SM / "results/tune"
# 설정 이름 → 템플릿의 (절, 키). a/b 값은 키 둘에 나눠 건다
FUSE = {
    "read": [("storage.traffic_control.read", "max_concurrent_requests"), ("storage.traffic_control.read", "max_concurrent_requests_per_server")],
    "batch": [("storage.traffic_control.read", "max_batch_bytes")],
    "rdma": [("storage.net_client", "enable_rdma_control")],
    "write": [("storage.traffic_control.write", "max_concurrent_requests"), ("storage.traffic_control.write", "max_concurrent_requests_per_server")],
    "wbatch": [("storage.traffic_control.write", "max_batch_bytes")],
    "conn": [("storage.net_client.io_worker.transport_pool", "max_connections")],
    "mct": [("storage.net_client.rdma_control", "max_concurrent_transmission")],
    "tsel": [("storage_io.read.targetSelection", "mode")],
}
FUSE_KEYS = tuple(FUSE)
QUOTED = ("batch", "wbatch", "tsel")
ST_KEYS = ("rts", "coro")


class NotAllowed(Exception):
    pass


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def literals(c):
    """설정의 FUSE 값을 템플릿 · 로그에 찍히는 글자로"""
    out = {}
    for k, places in FUSE.items():
        vs = c[k].split("/") if len(places) == 2 else [c[k]]
        for p, v in zip(places, vs):
            out[p] = f"'{v}'" if k in QUOTED else v
    return out


def tpl_values(text):
    """템플릿 글에서 설정 값을 읽는다"""
    t = tomllib.loads(text)

    def get(sec, key):
        d = t
        for s in sec.split("."):
            d = d[s]
        return str(d[key]).lower() if isinstance(d[key], bool) else str(d[key])
    return {k: "/".join(get(*p) for p in places) for k, places in FUSE.items()}


HOME_TPL = tpl_values(C.fuse_template().read_text())
WRITE = os.environ.get("FUSE_WRITE", HOME_TPL["write"])
WBATCH = os.environ.get("FUSE_WBATCH", HOME_TPL["wbatch"])
HOME = dict(HOME_TPL, rts=C.C["home"]["rts"], coro=C.C["home"]["coro"], write=WRITE, wbatch=WBATCH)
REF = dict(C.C["ref"])
BASE = dict(REF, write=WRITE, wbatch=WBATCH)
if not C.allowed("fuse_template"):
    BASE.update({k: HOME[k] for k in FUSE_KEYS})
if not C.allowed("storage_hot_update"):
    BASE.update({k: HOME[k] for k in ST_KEYS})
ST_HOME = dict(zip(("writes", "reads"), C.C["home"]["st_rdma"].split("/")))
CR_HOME = C.C["home"]["coro_read"]
CUR = None                  # 첫 apply 가 모두 명시적으로 건다


# --- FUSE ---
def versions():
    """우리 클라이언트의 mgmtd 설정 버전. 목록 밖 FUSE 클라이언트가 있으면 템플릿을 바꾸지 않는다 — 템플릿은 모든 FUSE 클라이언트에 걸린다"""
    v, others = {}, []
    for l in C.admin("list-clients").splitlines()[1:]:
        f = l.split()
        if len(f) > 8:
            (v.__setitem__(f[8], int(f[7])) if f[8] in C.CLIENT_SET else others.append(f[8]))
    if others:
        raise SystemExit(f"우리 목록 밖의 FUSE 클라이언트가 있다({', '.join(sorted(set(others)))}) — FUSE 템플릿을 바꾸면 그쪽에도 걸린다")
    return v


def make_tpl(c, out):
    want, sec, hit = literals(c), None, set()
    lines = C.fuse_template().read_text().split("\n")
    for i, l in enumerate(lines):
        m = re.match(r"^\[(.+)\]$", l)
        if m:
            sec = m.group(1)
            continue
        k = l.split(" = ")[0]
        if (sec, k) in want:
            lines[i] = f"{k} = {want[(sec, k)]}"
            hit.add((sec, k))
    if hit != set(want):
        raise SystemExit(f"템플릿에 없는 키: {set(want) - hit}")
    out.write_text("\n".join(lines))
    return out


def effective():
    """클라이언트마다 FUSE 로그에 마지막으로 찍힌 값(절, 키) → 글자. 설정 덤프는 스레드마다 따로 찍혀 스레드별로 절을 따라간다"""
    keys = " ".join(f"{s}/{k}" for places in FUSE.values() for s, k in places)
    awk = ("awk -v want='" + keys + "' 'BEGIN {n = split(want, w, \" \"); for (i = 1; i <= n; i++) W[w[i]] = 1} "
           "/ConfigManager.cc:[0-9]+ INFO\\] / {th = $2; sub(/.*ConfigManager.cc:[0-9]+ INFO\\] /, \"\"); "
           "if ($0 ~ /^\\[.*\\]$/) {sec[th] = substr($0, 2, length($0) - 2); next} "
           "split($0, a, \" = \"); k = sec[th] \"/\" a[1]; if (k in W) v[k] = a[2]} END {for (k in v) print k \" = \" v[k]}'")
    out = {}
    for c in C.CLIENTS:
        txt = C.sudo_sh(C.addr(c["host"]), f"grep -E 'ConfigManager.cc:[0-9]+ INFO\\]' {C.C['threefs']['fuse_log']} | {awk}")
        out[c["host"]] = {tuple(l.split(" = ")[0].split("/")): l.split(" = ", 1)[1] for l in txt.splitlines() if " = " in l}
    return out


def show_fuse(eff):
    one = lambda e: " ".join(f"{k} " + "/".join(e.get(p, "?").strip("'") for p in FUSE[k]) for k in FUSE_KEYS)
    return " | ".join(f"{h} {one(e)}" for h, e in eff.items())


def push_fuse(c):
    if not C.allowed("fuse_template"):
        raise NotAllowed("FUSE 템플릿")
    before = versions()
    same = all(c[k] == HOME_TPL[k] for k in FUSE_KEYS)
    src = C.fuse_template() if same else make_tpl(c, TMP / "fuse-tpl.toml")
    desc = "settings-" + "-".join(c[k].replace("/", "_") for k in FUSE_KEYS)
    C.admin(f"set-config --type FUSE --file {C.put_admin(src, '/tmp/sm-fuse-tpl.toml')} --desc {desc}")
    for _ in range(60):
        now = versions()
        if all(now.get(h, 0) > v for h, v in before.items()):
            break
        time.sleep(2)
    else:
        # 10-22: FUSE 를 다시 띄우면 mgmtd 에 예전 세션이 남고, 같은 내용을 다시 올리면 새 버전이 안 생겨 이 확인이 실패한다.
        # 맞는지는 아래의 실제 값 확인(FUSE 로그)이 가른다
        log(f"  !! 템플릿 {desc} 를 모든 클라이언트가 받지 못했다({versions()}) — 실제 값으로 확인한다")
    want = literals(c)
    for _ in range(12):                       # FUSE 로그에 새 설정이 찍힐 때까지 조금 걸릴 수 있다
        eff = effective()
        if all(all(e.get(p) == v for p, v in want.items()) for e in eff.values()):
            return show_fuse(eff)
        time.sleep(5)
    raise RuntimeError("FUSE 설정이 모든 클라이언트에 걸리지 않았다:\n" + show_fuse(eff))


# --- storage ---
def _get(text, sec, key):
    m = re.search(rf"^\[{re.escape(sec)}\]\n(?:[^\[].*\n)*?{key} = (.+)$", text, re.M)
    return m.group(1).strip("'\"") if m else None


def storage_state():
    """노드마다 지금 값: rts(on|off) coro(쓰기 coroutine) coro_read st_rdma(writes/reads)"""
    out = {}
    for s in C.STORAGE:
        t = C.storage_config(s["node_id"])
        pool = lambda sec: f"{_get(t, sec, 'coroutines_num')}/{_get(t, sec, 'threads_num')}"
        out[s["node_id"]] = {"rts": "off" if _get(t, "server.storage", "rdma_transmission_req_timeout") in ("0ns", "0ms", None) else "on",
                             "coro": pool("server.coroutines_pool_update"), "coro_read": pool("server.coroutines_pool_read"),
                             "st_rdma": f"{_get(t, 'server.storage', 'max_concurrent_rdma_writes')}/{_get(t, 'server.storage', 'max_concurrent_rdma_reads')}"}
    return out


def hot_update(name, text, want):
    """storage 모두에 hot-update 하고, want(키 → 값)가 모든 노드에 걸릴 때까지 본다"""
    if not C.allowed("storage_hot_update"):
        raise NotAllowed("storage hot-update")
    f = TMP / f"st-{name}.toml"
    f.write_text(text)
    C.admin(f"hot-update-config -t STORAGE -f {C.put_admin(f, f'/tmp/sm-st-{name}.toml')}")
    for _ in range(6):
        st = storage_state()
        if all(all(s[k] == v for k, v in want.items()) for s in st.values()):
            return st
        time.sleep(5)
    raise RuntimeError(f"storage {name} 가 모든 노드에 걸리지 않았다: {st}")


def set_storage(c, force):
    if force or c["rts"] != CUR["rts"]:
        hot_update(f"rts-{c['rts']}", f"[server.storage]\nrdma_transmission_req_timeout = '{'1s' if c['rts'] == 'on' else '0ns'}'\n", {"rts": c["rts"]})
    if force or c["coro"] != CUR["coro"]:
        a, b = c["coro"].split("/")
        hot_update(f"coro-{a}-{b}", f"[server.coroutines_pool_update]\ncoroutines_num = {a}\nthreads_num = {b}\n", {"coro": c["coro"]})
    st = storage_state()
    if not all(s["rts"] == c["rts"] and s["coro"] == c["coro"] for s in st.values()):
        raise RuntimeError(f"storage 가 원한 값(rts {c['rts']} · coro {c['coro']})이 아니다: {st}")
    return st


def set_st(writes, reads):
    """storage RDMA 동시 수(storage NIC 마다): writes = load 응답을 써 주는 쪽, reads = store 를 당겨 오는 쪽"""
    want = f"{writes}/{reads}"
    if all(s["st_rdma"] == want for s in storage_state().values()):
        return
    hot_update(f"rdma-{writes}-{reads}", f"[server.storage]\nmax_concurrent_rdma_writes = {writes}\nmax_concurrent_rdma_reads = {reads}\n", {"st_rdma": want})
    log(f"  storage RDMA 동시 수 writes {writes} · reads {reads} (모든 노드 같음)")


def set_cr(v):
    """storage 의 읽기 요청 coroutine pool(server.coroutines_pool_read)"""
    if all(s["coro_read"] == v for s in storage_state().values()):
        return
    a, b = v.split("/")
    hot_update(f"coro-read-{a}-{b}", f"[server.coroutines_pool_read]\ncoroutines_num = {a}\nthreads_num = {b}\n", {"coro_read": v})
    log(f"  storage 읽기 coroutine {v} (모든 노드 같음)")


def apply(c, force=False):
    """설정 c 를 건다. 지금과 다른 쪽만 바꾼다(첫 호출 · force 는 모두). 허락 없는 쪽을 바꿔야 하면 NotAllowed"""
    global CUR
    first = CUR is None or force
    fuse = st = None
    if not C.allowed("fuse_template") and any(c[k] != HOME[k] for k in FUSE_KEYS):
        raise NotAllowed("FUSE 템플릿")
    if not C.allowed("storage_hot_update") and any(c[k] != HOME[k] for k in ST_KEYS):
        raise NotAllowed("storage hot-update")
    if C.allowed("fuse_template") and (first or any(c[k] != CUR[k] for k in FUSE_KEYS)):
        fuse = push_fuse(c)
    if C.allowed("storage_hot_update") and (first or any(c[k] != CUR[k] for k in ST_KEYS)):
        st = next(iter(set_storage(c, first).values()))
    CUR = dict(c)
    log(f"  설정 {c}" + (f"\n    FUSE: {fuse}" if fuse else "") + (f"\n    storage(모든 노드 같음): {st}" if st else ""))


def check_home():
    """시작 때 클러스터가 되돌릴 곳(HOME)에 있는지 본다 — 아니면 되돌리기가 남의 값을 지운다. 다르면 멈춘다"""
    bad = []
    if C.allowed("fuse_template"):
        tmp = "/tmp/sm-fuse-now.toml"
        C.admin(f"get-config -t FUSE -o {tmp}")
        now = C.sudo_sh(C.C["threefs"]["admin_host"], f"cat {tmp}; rm -f {tmp}")
        diff = {k: v for k, v in tpl_values(now).items() if v != HOME[k]}     # 쓰기 둘은 FUSE_WRITE · FUSE_WBATCH 로 알려 준 값과 견준다
        if diff:
            bad.append(f"mgmtd FUSE 템플릿이 되돌릴 곳(사본 {C.fuse_template().name} · FUSE_WRITE · FUSE_WBATCH)과 다르다: {diff}")
    if C.allowed("storage_hot_update"):
        home = {k: C.C["home"][k] for k in ("rts", "coro", "coro_read", "st_rdma")}
        for n, s in storage_state().items():
            if s != home:
                bad.append(f"storage {n} {s} ≠ [home] {home} (storage 가 다시 떠 hot-update 가 풀렸을 수 있다)")
    return bad


def restore():
    log("== 되돌리기")
    if C.allowed("storage_hot_update"):
        set_st(**ST_HOME)
        set_cr(CR_HOME)
    apply(HOME, force=True)


def state():
    print("FUSE  " + show_fuse(effective()))
    for n, s in storage_state().items():
        print(f"storage {n} {s}")
    pids = C.storage_pids()
    print("storage PID", pids)


TMP.mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "state"
    if cmd == "restore":
        restore()
    elif cmd == "home":
        print(check_home() or "HOME 과 같다")
    else:
        state()
