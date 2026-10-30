#!/usr/bin/env python3
"""클러스터 고유 값(호스트 · 계정 · 3FS 경로 · storage node · 천장)을 한 파일에서 읽는다.
도구의 다른 코드는 이런 값을 직접 쓰지 않고 여기서 받는다.

파일은 clusters/<이름>.toml 이다. 이 장비의 hostname 이 [[client]] 에 있는 파일을 고른다(SM_CLUSTER=<이름> 으로 정할 수도 있다).
빈칸("")이 남아 있으면 쓰기 전에 멈추고 그 항목을 나열한다. 새 클러스터는 clusters/TEMPLATE.toml 을 복사해
[access] 를 채운 뒤 discover 로 나머지를 제안받고, 사람이 확인한다.

    cluster.py discover NAME   이 장비 · admin_cli · 각 노드에서 값을 찾아 clusters/NAME.toml(과 FUSE 템플릿 사본)을 낸다
    cluster.py check           모든 노드의 접속 · sudo · admin_cli · 마운트 · 재생 폴더 쓰기 · NIC 를 본다
    cluster.py env             bash 용 변수(lib.sh · throttle.sh 가 eval 한다)
    cluster.py show            고른 파일과 값
"""
import copy
import json
import os
import re
import shlex
import socket
import subprocess
import sys
import tomllib
from pathlib import Path

HERE = Path(__file__).resolve().parent
DIR = HERE / "clusters"
ME = socket.gethostname()
ACTIONS = ("fuse_template", "storage_hot_update", "fuse_restart", "throttle", "other_fuse")


def _pick():
    name = os.environ.get("SM_CLUSTER")
    if name:
        return DIR / f"{name}.toml"
    hit = [f for f in sorted(DIR.glob("*.toml")) if f.name != "TEMPLATE.toml" and not f.stem.endswith("-fuse")
           and any(c.get("host") == ME for c in tomllib.loads(f.read_text()).get("client", []))]
    if len(hit) > 1:
        raise SystemExit(f"이 장비({ME})가 든 클러스터 파일이 둘 이상이다({', '.join(f.name for f in hit)}) — SM_CLUSTER=<이름> 으로 고른다")
    if not hit:
        raise SystemExit(f"이 장비({ME})가 클라이언트로 든 클러스터 파일이 {DIR} 에 없다 — TEMPLATE.toml 을 복사해 채운다(cluster.py discover)")
    return hit[0]


def _blanks(x, at=""):
    if isinstance(x, dict):
        return [b for k, v in x.items() for b in _blanks(v, f"{at}.{k}" if at else k)]
    if isinstance(x, list):
        return [b for i, v in enumerate(x) for b in _blanks(v, f"{at}[{i}]")] or ([at] if not x else [])
    return [at] if x == "" else []


PATH = _pick()
C = tomllib.loads(PATH.read_text())
_empty = [b for b in _blanks(C) if not b.startswith("access.") or not b.endswith("password_file")]
if _empty and not (len(sys.argv) > 1 and sys.argv[0].endswith("cluster.py") and sys.argv[1] == "discover"):
    raise SystemExit(f"{PATH.name} 에 빈칸이 있다: " + ", ".join(_empty))

NAME = C["name"]
MOUNT = C["threefs"]["mount"]
CLIENTS = C.get("client", [])
STORAGE = C.get("storage", [])
NODES = {s["host"]: s for s in CLIENTS + STORAGE}       # 같은 장비가 클라이언트이자 storage 면 storage 쪽 항목(nvme 가 있다)
CLIENT_SET = {c["host"] for c in CLIENTS}


def data(sub=""):
    """3FS 위 우리 폴더(data_root) 아래 경로"""
    return f"{MOUNT}/{C['threefs']['data_root']}" + (f"/{sub}" if sub else "")


def rel(path):
    """마운트 기준 경로(admin_cli 가 받는 것)"""
    if not path.startswith(MOUNT + "/"):
        raise ValueError(f"{path} 는 3FS 마운트 {MOUNT} 아래가 아니다")
    return path[len(MOUNT):]


def addr(host):
    """이 장비면 None(로컬에서 돈다), 아니면 관리망 주소"""
    return None if host == ME else NODES[host]["addr"]


def _pw(kind):
    f = C["access"].get(f"{kind}_password_file", "")
    return Path(os.path.expanduser(f)).read_text().strip() if f else None


SSH = (["sshpass", "-f", os.path.expanduser(C["access"]["ssh_password_file"])] if C["access"].get("ssh_password_file") else []) + \
      ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10"] + ([] if C["access"].get("ssh_password_file") else ["-o", "BatchMode=yes"])


def ssh(a):
    """a 에 접속하는 argv 앞부분(뒤에 원격 명령을 붙인다)"""
    return SSH + [f"{C['access']['ssh_user']}@{a}"]


def sudo():
    """원격 명령 앞에 붙일 sudo. 비밀번호 파일이 없으면 NOPASSWD 로 본다"""
    pw = _pw("sudo")
    return f"echo {shlex.quote(pw)} | sudo -S -p ''" if pw else "sudo -n"


# 원격에서 replay · 표본 · 파일 복사를 돌릴 계정. ssh_user 와 다르면 ssh_user 가 비밀번호 없이 그 계정이 된다(sudo -n -u).
# 그 계정의 login 환경과 $HOME 으로 돈다. root 명령(admin_cli · FUSE 로그 · storage 설정)은 늘 ssh_user 의 sudo 로
RUNS_AS = C["access"].get("runs_as", "")
if RUNS_AS and C["access"].get("sudo_password_file"):
    raise SystemExit("access.runs_as 는 ssh_user 가 비밀번호 없이 그 계정이 될 때만 쓴다(sudo -n -u) — sudo_password_file 을 비운다")


def remote(a, cmd):
    """a(None 이면 이 장비)에서 bash 명령을 도는 argv"""
    if a is None:
        return ["bash", "-c", cmd]
    return ssh(a) + [f"sudo -n -u {RUNS_AS} -H bash -lc {shlex.quote(cmd)}" if RUNS_AS else cmd]


def _run(argv, what, check):
    r = subprocess.run(argv, capture_output=True, text=True)
    if check and r.returncode:
        raise RuntimeError(f"{what[:120]}… 실패 {r.returncode}: {r.stderr.strip()[:300]}")
    return r.stdout


def run_on(a, cmd, **kw):
    """a(None 이면 이 장비)에서 bash 명령. Popen"""
    return subprocess.Popen(remote(a, cmd), **kw)


def sh(a, cmd, check=True):
    return _run(remote(a, cmd), f"{a or ME}: {cmd}", check)


def sudo_sh(a, cmd, check=True):
    """root 로 돈다. 이 장비에서도 ssh_user 로 접속해 sudo 한다(작업 계정에 sudo 가 없을 수 있다)"""
    return _run(ssh(a or "localhost") + [f"{sudo()} bash -c {shlex.quote(cmd)}"], f"{a or ME}: sudo {cmd}", check)


def admin(cmd):
    return _run(ssh(C["threefs"]["admin_host"]) + [f"{sudo()} {C['threefs']['admin_cli']} {shlex.quote(cmd)} 2>/dev/null"],
                f"admin_cli {cmd}", True)


def put_admin(local, remote):
    """admin_cli 가 읽을 파일을 admin_host 에 둔다(admin_host 가 이 장비면 그대로)"""
    h = C["threefs"]["admin_host"]
    if h in ("localhost", ME):
        return str(local)
    subprocess.run(ssh(h) + [f"cat > {remote}"], input=Path(local).read_text(), text=True, check=True)
    return remote


def allowed(action):
    assert action in ACTIONS, action
    return bool(C["allow"].get(action, False))     # 없는 키는 허락 안 함(나중에 생긴 항목)


def require(action, what):
    if not allowed(action):
        raise SystemExit(f"{NAME}: {what} 은 [allow] {action} = false 라 하지 않는다")


def fuse_template():
    return HERE / C["threefs"]["fuse_template"]


def storage_config(node_id):
    """storage 노드 하나가 지금 쓰는 설정(hot-update 포함). admin_host 에 받아 읽고 지운다"""
    f = f"/tmp/sm-cfg-{node_id}.toml"
    admin(f"get-config -n {node_id} -o {f}")
    return sudo_sh(C["threefs"]["admin_host"], f"cat {f}; rm -f {f}")


def storage_pids():
    """storage node 번호 → storage 프로세스 PID(mgmtd 가 아는 것). 실행 앞뒤가 다르면 그 사이에 storage 가 다시 떴다"""
    out = {}
    for l in admin("list-nodes").splitlines():
        f = l.split()
        if len(f) > 4 and f[1] == "STORAGE":
            out[int(f[0])] = int(f[4])
    return out


def calibration():
    """calibration.json(소프트웨어의 성질 · PoC 기록) 위에 이 클러스터의 값을 얹는다. driver 에도 이것을 보낸다"""
    cal = json.loads((HERE / "calibration.json").read_text())
    m, d = C["measured"], cal["util_denominator"]
    cal["cluster"] = NAME
    cal["clients"] = [{"host": c["host"], "mgmt": c["addr"], "runtime": True, "python": c["python"], "workdir": c["workdir"]} for c in CLIENTS]
    cal["storage_nodes"] = len(STORAGE)
    for k in ("disk_read", "disk_write", "link"):          # 천장은 사용률(%)에만 쓴다. 없으면 None — 사용률이 빈다
        cal["ceilings_gibps"][d[k]] = dict(cal["ceilings_gibps"][d[k]], value=m.get(f"{k}_gibps"))
    tfs = DIR / f"{PATH.stem}-ceiling.json"                       # 3FS 경로 천장(ceiling.py 가 잰다)
    cal["threefs_ceiling"] = json.loads(tfs.read_text()) if tfs.exists() else None
    cal["replicas"] = dict(cal["replicas"], value=m["replicas"])
    # 클라이언트 한 대 한계(plan.py 가 넘는 배속을 빼고 "max" 를 정한다). 그 클러스터에서 잰 값이 없으면 자르지 않는다
    cal["client_wall"] = dict(cal["client_wall"], paced_gibps=m.get("client_load_wall_gibps"))
    cal["store_wall"] = dict(cal["store_wall"], overlap_gibps=m.get("client_store_wall_gibps"))
    cal["layout"] = dict(cal["layout"], chains=m["chains"], default_stripe=m["default_stripe"], chunk_bytes=m["chunk_bytes"])
    cal["adapter"] = copy.deepcopy(cal["adapter"])
    cal["adapter"]["backend_params"]["mount_point"] = MOUNT
    return cal


# netdev 하나의 RDMA 장치 · 포트 상태 · 링크 · IPv4. 3FS 는 RDMA 포트가 ACTIVE 면 되고 IPv4 는 없어도 된다(InfiniBand 등)
NIC_STATE = ("ib=$(ls /sys/class/net/{nd}/device/infiniband 2>/dev/null | head -1); "
             "echo \"$ib|$(cat /sys/class/infiniband/$ib/ports/1/state 2>/dev/null)|$(cat /sys/class/net/{nd}/operstate 2>/dev/null)|"
             "$(ip -br -4 addr show {nd} 2>/dev/null | awk '{{print $3}}')\"")
# RDMA 포트가 ACTIVE 인 netdev 들
ACTIVE_NICS = ("for d in /sys/class/infiniband/*; do grep -q ACTIVE $d/ports/1/state 2>/dev/null || continue; "
               "for n in $d/device/net/*; do [ -e \"$n\" ] && basename $n; done; done")


# --- 명령 ---
def env():
    st, cl = STORAGE, CLIENTS
    if len({tuple(s["nvme"]) for s in st}) != 1:
        raise SystemExit("storage 노드마다 NVMe 목록이 다르다 — throttle.sh 는 같은 목록을 가정한다")
    q = shlex.quote
    arr = lambda xs: "(" + " ".join(q(str(x)) for x in xs) + ")"
    print(f"SM_CLUSTER={q(NAME)}")
    print(f"SSH={q(' '.join(q(x) for x in SSH))}")
    print(f"SUSER={q(C['access']['ssh_user'])}")
    print(f"SUDO={q(sudo())}")
    print(f"ADMIN_HOST={q(C['threefs']['admin_host'])}")
    print(f"ADMIN_CLI={q(C['threefs']['admin_cli'])}")
    print(f"MOUNT={q(MOUNT)}")
    print(f"LOG={q(C['threefs']['fuse_log'])}")
    print(f"FUSE_SERVICE={q(C['threefs']['fuse_service'])}")
    print(f"FUSE_TEMPLATE={q(str(fuse_template()))}")
    print(f"CLI3={arr('localhost' if c['host'] == ME else c['addr'] for c in cl)}")
    print(f"CLI_IF={arr(c['netdevs'][0] for c in cl)}")
    print(f"STOR={arr(s['addr'] for s in st)}")
    print(f"STOR_IF={arr(s['netdevs'][0] for s in st)}")
    print(f"NODES={arr(s['node_id'] for s in st)}")
    print(f"NVME={arr(st[0]['nvme'])}")
    for a in ACTIONS:
        print(f"ALLOW_{a.upper()}={int(allowed(a))}")


def check():
    bad = []

    def ok(what, cond, detail=""):
        print(f"  {'ok' if cond else '!!'} {what}" + (f" — {detail}" if detail else ""))
        if not cond:
            bad.append(what)

    print(f"{PATH.name} ({NAME}), 이 장비 {ME}")
    try:
        nodes = admin("list-nodes")
        ok("admin_cli list-nodes", "STORAGE" in nodes)
        pids = storage_pids()
        ok("storage node 번호", sorted(pids) == sorted(s["node_id"] for s in STORAGE), f"mgmtd {sorted(pids)}")
    except Exception as e:
        ok("admin_cli", False, str(e)[:200])
    for h, n in NODES.items():
        a = addr(h)
        try:
            ok(f"{h} 접속", sh(a, "hostname").strip() == h)
            ok(f"{h} sudo", sudo_sh(a, "id -u").strip() == "0")
            for nd in n["netdevs"]:
                ib, st, op, ip4 = (sh(a, NIC_STATE.format(nd=nd), check=False).strip().split("|") + ["", "", "", ""])[:4]
                ok(f"{h} {nd} RDMA 포트", bool(ib) and "ACTIVE" in st, f"{ib or 'RDMA 장치 없음'} {st} · {op} · {ip4 or 'IPv4 없음'}")
            if h in CLIENT_SET:
                c = next(x for x in CLIENTS if x["host"] == h)
                ok(f"{h} 3FS 마운트", f" {MOUNT} fuse.hf3fs " in sh(a, "cat /proc/mounts"))
                ok(f"{h} python · workdir", sh(a, f"test -x {c['python']} && test -d {c['workdir']} && echo y", check=False).strip() == "y",
                   f"{c['python']} {c['workdir']}")
                t = data(f".check-{h}")
                ok(f"{h} 재생 폴더 쓰기", sh(a, f"mkdir -p {data()} && touch {t} && rm -f {t} && echo y", check=False).strip() == "y", data())
            if "nvme" in n:
                devs = sh(a, "lsblk -dno NAME,TYPE | awk '$2==\"disk\" {print \"/dev/\"$1}'", check=False).split()
                ok(f"{h} NVMe", set(n["nvme"]) <= set(devs), " ".join(devs))
        except Exception as e:
            ok(f"{h}", False, str(e)[:200])
    ok("FUSE 템플릿 사본", fuse_template().exists(), str(fuse_template()))
    # 천장은 돌리는 데 필요 없다(참고 값). 없으면 알리기만 한다
    blank = [k for k in ("disk_read_gibps", "disk_write_gibps", "link_gibps") if not C["measured"].get(k)]
    print("  -- 디스크 · 링크 천장 " + (f"없음({', '.join(blank)}) — 사용률(%)이 빈다" if blank else "있음"))
    tfs = DIR / f"{PATH.stem}-ceiling.json"
    t = json.loads(tfs.read_text()) if tfs.exists() else None
    print("  -- 3FS 경로 천장 " + (f"읽기 {t['read_gibps']:.2f} · 쓰기 {t['write_gibps']:.2f} GiB/s ({t['measured_at']})" if t
                                  else "없음 — python3 ceiling.py 로 잰다"))
    hw = (t or {}).get("hardware")
    print("  -- 하드웨어 천장 " + (f"읽기 {hw['read_gibps']:.2f} · 쓰기 {hw['write_gibps']:.2f} GiB/s(먼저 차는 곳 {hw['read_wall']} · {hw['write_wall']})" if hw
                               else "없음 — python3 ceiling.py 가 함께 계산한다"))
    print("통과" if not bad else f"안 된 것 {len(bad)}: " + ", ".join(bad))
    return not bad


def discover(name):
    """admin_cli 와 각 노드에서 찾을 수 있는 값을 채운 초안을 낸다. [access] 는 먼저 채워 둔다.
    비어 있는 값은 기본값을 찾아 본다: admin_cli 는 3FS 기본 설치 경로, admin_host 는 이 장비, data_root 는 <계정>/scale-model,
    클라이언트의 python 은 $HOME/venv-lmcache(-hf3fs), workdir 는 이 도구가 있는 곳. 찾은 값에는 '# 확인' 을 붙인다.
    한 노드에 3FS 클러스터가 여럿 붙어 있을 수 있어 마운트 · FUSE 로그 · FUSE 서비스는 admin_cli 설정의 cluster_id 로 고른다"""
    t = C["threefs"]
    if not t.get("admin_host"):
        t["admin_host"] = "localhost"
    if not t.get("admin_cli"):
        b, c = "/opt/3fs/bin/admin_cli", "/opt/3fs/etc/admin_cli.toml"
        if sudo_sh(t["admin_host"], f"test -x {b} && test -f {c} && echo y", check=False).strip() != "y":
            raise SystemExit(f"admin_cli 를 {t['admin_host']} 의 {b} 에서 찾지 못했다 — threefs.admin_cli · admin_host 를 채운다")
        t["admin_cli"] = f"{b} -cfg {c}"
    out = DIR / f"{name}.toml"

    def cluster_id(line):
        m = re.search(r"cluster_id\s*=\s*['\"]?([^'\"\s]+)", line)
        return m.group(1) if m else ""

    cfg = re.search(r"-cfg[ =](\S+)", t["admin_cli"])
    cid = cluster_id(sudo_sh(t["admin_host"], f"grep -m1 '^cluster_id' {cfg.group(1)}", check=False)) if cfg else ""
    print(f"{PATH.name} 의 [access] · admin_cli({t['admin_host']}: {t['admin_cli']}) 로 찾는다. cluster_id = {cid or '못 찾음'}")
    nodes = [l.split() for l in admin("list-nodes").splitlines()[1:] if l.split()]
    stor = sorted((int(f[0]), f[3]) for f in nodes if f[1] == "STORAGE")
    clients = sorted({f[8] for f in (l.split() for l in admin("list-clients").splitlines()[1:]) if len(f) > 8 and f[8] != "N/A"})   # N/A: FUSE 가 아닌 클라이언트(raw_hf3fs connector 등)
    res = lambda h: (subprocess.run(["getent", "hosts", h], capture_output=True, text=True).stdout.split() or [""])[0]

    def probe(h, client=False):
        a = None if h == ME else res(h)
        try:
            nd = sh(a, ACTIVE_NICS, check=False).split()
            p = {"nd": nd, "ip4": [n for n in nd if sh(a, f"ip -br -4 addr show {n} 2>/dev/null | awk '{{print $3}}'", check=False).strip()],
                 "nv": sh(a, "lsblk -dno NAME,TYPE | awk '$2==\"disk\" && $1 ~ /^nvme/ {print \"/dev/\"$1}'", check=False).split(),
                 "mnt": [f[1] for f in (l.split() for l in sh(a, "cat /proc/mounts").splitlines()) if f[0] == f"hf3fs.{cid}" and f[2] == "fuse.hf3fs"]}
            if client:
                p["py"] = sh(a, 'for p in "$HOME/venv-lmcache/bin/python" "$HOME/venv-lmcache-hf3fs/bin/python"; do '
                                '[ -x "$p" ] && { echo "$p"; break; }; done', check=False).strip()
                p["wd"] = sh(a, f'for d in {shlex.quote(str(HERE))} "$HOME/scale-model"; do '
                                '[ -f "$d/driver.py" ] && { echo "$d"; break; }; done', check=False).strip()
                # ExecStart 에 hf3fs_fuse_main 이 있는 unit 과, 그 --launcher_cfg 의 cluster_id 줄
                rows = sudo_sh(a, "for u in $(systemctl list-units --all --type=service --no-legend --plain | awk '{print $1}'); do "
                               'e=$(systemctl show -p ExecStart --value "$u"); case "$e" in *hf3fs_fuse_main*) ;; *) continue ;; esac; '
                               "c=$(echo \"$e\" | grep -oE -- '--launcher_cfg[ =][^ ;]+' | head -1 | cut -c16-); "
                               'echo "${u%.service}" "$(grep -m1 "^cluster_id" "$c" 2>/dev/null)"; done', check=False).splitlines()
                units = [(r.split()[0], cluster_id(r)) for r in rows if r.split()]
                p["svcs"] = [f"{u} {c or '?'}" for u, c in units]
                # 다른 클러스터의 FUSE 를 restart 하지 않도록 cluster_id 가 맞는 것만 고른다
                p["svc"] = next((u for u, c in units if cid and c == cid), "")
                d = shlex.quote(f"/var/log/3fs-{cid}")
                p["log"] = sudo_sh(a, f"for d in {d} /var/log/3fs; do ls $d/hf3fs_fuse_main.log 2>/dev/null || ls $d/*fuse*.log 2>/dev/null | head -1; done | head -1",
                                   check=False).strip()
            return p
        except Exception as e:
            print(f"  {h}: 접속 못 함({str(e)[:80]}) — 그 노드 값은 빈칸")
            return {"nd": [], "ip4": [], "nv": [], "mnt": [], "py": "", "wd": "", "svc": "", "svcs": [], "log": ""}

    cp = {h: probe(h, client=True) for h in clients}
    me = cp.get(ME) or probe(ME, client=True)
    first = next(iter(cp.values()), me)
    fuse = DIR / f"{name}-fuse.toml"
    tmp = "/tmp/sm-fuse-tpl.toml"
    admin(f"get-config -t FUSE -o {tmp}")
    fuse.write_text(sudo_sh(t["admin_host"], f"cat {tmp}; rm -f {tmp}"))
    st0 = storage_config(stor[0][0]) if stor else ""

    def get(sec, key, text=st0):
        m = re.search(rf"^\[{re.escape(sec)}\]\n(?:[^\[].*\n)*?{key} = (.+)$", text, re.M)
        return m.group(1).strip("'\"") if m else ""

    lay = {l.split()[0]: l.split()[-1] for l in admin("stat --display-layout /").splitlines() if l.startswith("Layout-")}
    chains = [l for l in admin("list-chains").splitlines()[1:] if l.split()]
    q = lambda v: json.dumps(v, ensure_ascii=False)
    found = lambda v, what: f"{q(v)}   # 확인 — {what}" if v else f"{q('')}   # 못 찾음 — {what}"
    root = t.get("data_root") or f"{RUNS_AS or C['access']['ssh_user']}/scale-model"
    sp = me if me.get("svc") else first
    svc_what = f"ExecStart 에 hf3fs_fuse_main, launcher_cfg 의 cluster_id = {cid}" + (f" (후보 unit · cluster_id: {', '.join(sp['svcs'])})" if sp.get("svcs") else "")
    L = [f"# cluster.py discover 가 {ME} 에서 낸 초안. 빈칸과 '# 확인' 을 사람이 본다. 항목의 뜻은 TEMPLATE.toml", f"name = {q(name)}", "",
         "[access]"] + [f"{k} = {q(v)}" for k, v in C["access"].items()] + ["", "[threefs]",
         f"mount = {found((me['mnt'] or first['mnt'] or [''])[0], f'/proc/mounts 첫 칸이 hf3fs.{cid}')}",
         f"data_root = {found(root, '마운트 아래 우리만 쓰는 폴더')}",
         f"admin_cli = {q(t['admin_cli'])}", f"admin_host = {q(t['admin_host'])}",
         f"fuse_log = {found(me.get('log') or first.get('log'), f'/var/log/3fs-{cid}, 없으면 /var/log/3fs')}",
         f"fuse_service = {found(sp.get('svc'), svc_what)}",
         f"fuse_template = {q(str(fuse.relative_to(HERE)))}   # 지금 mgmtd 의 FUSE 템플릿(되돌릴 곳)", "",
         "[allow]   # 클러스터 주인에게 묻고 채운다", "fuse_template = false", "storage_hot_update = false", "fuse_restart = false", "throttle = false", "other_fuse = false", "",
         "[home]   # 되돌릴 storage hot-update 값 — storage " + (str(stor[0][0]) if stor else "") + " 의 지금 값",
         f"rts = {q('on' if get('server.storage', 'rdma_transmission_req_timeout') not in ('', '0ns', '0ms') else 'off')}",
         f"coro = {q(get('server.coroutines_pool_update', 'coroutines_num') + '/' + get('server.coroutines_pool_update', 'threads_num'))}",
         f"coro_read = {q(get('server.coroutines_pool_read', 'coroutines_num') + '/' + get('server.coroutines_pool_read', 'threads_num'))}",
         f"st_rdma = {q(get('server.storage', 'max_concurrent_rdma_writes') + '/' + get('server.storage', 'max_concurrent_rdma_reads'))}", "",
         "[ref]   # 실험 기준 = 3FS 소스 기본값(10-21 사용자). 쓰기 둘은 FUSE 를 다시 띄워야 바뀌어 지금 걸린 값으로 돈다",
         'read = "32/8"', 'batch = "4MB"', 'rdma = "false"', 'conn = "1"', 'mct = "64"', 'rts = "off"',
         'coro = "64/8"', 'tsel = "Default"', "",
         "# 클라이언트 = list-clients 의 FUSE 클라이언트 전부. replay 에 쓰지 않을 노드(GPU 노드 등)는 지운다"]
    for h, p in cp.items():
        L += ["[[client]]", f"host = {q(h)}", f"addr = {q(res(h))}", f"python = {found(p['py'], 'LMCache 가 든 venv 의 python')}",
              f"workdir = {found(p['wd'], '이 도구 사본')}",
              f"netdevs = {q(p['nd'])}   # 확인 — RDMA 포트가 ACTIVE 인 것. 3FS 가 쓰는 것만 남긴다(IPv4 있는 것: {', '.join(p['ip4']) or '없음'})", ""]
    for nid, h in stor:
        p = cp.get(h) or probe(h)
        L += ["[[storage]]", f"host = {q(h)}", f"addr = {q(res(h))}", f"node_id = {nid}",
              f"netdevs = {q(p['nd'])}   # 확인(IPv4 있는 것: {', '.join(p['ip4']) or '없음'})", f"nvme = {q(p['nv'])}   # 확인 — 3FS target 이 든 것만", ""]
    L += ["[measured]   # 3FS 구성은 admin_cli 에서. 천장(선택, 사용률 %에만 쓴다)은 사양값이어도 된다 — 그때는 그렇게 적는다",
          '# disk_read_gibps = ""', '# disk_write_gibps = ""', '# link_gibps = ""   # 노드 하나의 RDMA 선로 합(GiB/s)',
          f"replicas = {max((len(re.findall(r'\d+\([A-Z-]+\)', c)) for c in chains), default=0) or q('')}   # 확인 — list-chains",
          f"chains = {len(chains) or q('')}", f"default_stripe = {lay.get('Layout-StripeSize') or q('')}", f"chunk_bytes = {lay.get('Layout-ChunkSize') or q('')}"]
    out.write_text("\n".join(L) + "\n")
    print(f"초안 {out}, FUSE 템플릿 {fuse}. 빈칸 · '# 확인' 을 채운 뒤 cluster.py check, 3FS 경로 천장은 ceiling.py")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "show"
    if cmd == "env":
        env()
    elif cmd == "check":
        sys.exit(0 if check() else 1)
    elif cmd == "discover":
        discover(sys.argv[2])
    else:
        print(PATH)
        print(json.dumps({k: v for k, v in C.items() if k != "access"}, ensure_ascii=False, indent=1))
