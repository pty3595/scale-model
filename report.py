#!/usr/bin/env python3
"""replay 결과를 읽는다 — sweep 곡선, 대상 클러스터의 운영점, 두 실행(최적화 전·후)의 비교.

  곡선     단계마다 사용률(storage 디스크 읽기·쓰기, 클라이언트·storage 링크)과 request 당 load 시간,
           request 시작 지연, storage NIC drop 을 표로 낸다
  운영점   --target 사양에서 대상 storage 노드의 디스크 읽기 사용률을 구해 곡선 위의 위치와 그 자리의 값을 보인다.
           대상의 클라이언트 링크 사용률 ÷ 디스크 읽기 사용률이 우리와 크게 다르면(먼저 차는 자원이 다르면) 경고한다
  비교     --baseline 의 같은 단계와 request 끼리 짝지어 load 시간 비율(중앙값, 95% 구간)을 낸다.
           --noise 에 기준을 한 번 더 돌린 결과를 주면 그 차이를 잡음으로 보고, 효과가 잡음 밖인지 판정한다

    python3 report.py results/coder
    python3 report.py results/coder --target targets/<대상>.json
    python3 report.py results/coder-stripe1 --baseline results/coder --noise results/coder-again
"""

import argparse
import json
import random
import statistics
from pathlib import Path

GIB, GB = 1024**3, 1000**3


def pct(xs, q):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(q * len(xs)))]


def levels(run: Path) -> dict:
    """단계 이름 → summary. 폴더 하나짜리 실행도 받는다."""
    if (run / "summary.json").exists():
        return {run.name: json.loads((run / "summary.json").read_text())}
    out = {d.name: json.loads((d / "summary.json").read_text())
           for d in run.iterdir() if (d / "summary.json").exists()}
    return dict(sorted(out.items(), key=lambda kv: kv[1]["util"]["disk_read"]))


def drops(s: dict) -> int:
    return sum(v for h, x in s["hw"].items() if h.startswith("storage") and "error" not in x
               for k, v in x["eth_events"].items() if "rx_discards_phy" in k)


def curve(lv: dict) -> None:
    print(f"{'단계':>9}  {'디스크 읽기':>6} {'쓰기':>6}  {'링크 클라':>6} {'storage':>7}  "
          f"{'load p50':>8} {'p99':>6}  {'시작 지연 p99':>8}  {'NIC drop':>9}")
    for name, s in lv.items():
        u, l = s["util"], s["load_s_per_request"]
        print(f"{name:>9}  {u['disk_read']:9.3f} {u['disk_write']:6.3f}  {u['client_link']:8.3f} {u['storage_link']:7.3f}  "
              f"{l['p50']:7.2f}s {l['p99']:5.2f}s  {s['late_s']['p99']:11.2f}s  {drops(s):9d}")


def to_gibps(spec: dict, name: str) -> float:
    """`<name>_gibps`(GiB/s), `<name>_gbps`(GB/s), `<name>_gbit`(Gbit/s)를 GiB/s 로 읽는다."""
    for unit, scale in (("gibps", 1), ("gbps", GB / GIB), ("gbit", GB / 8 / GIB)):
        if f"{name}_{unit}" in spec:
            return float(spec[f"{name}_{unit}"]) * scale
    raise KeyError(name)


def has(spec: dict, name: str) -> bool:
    return any(f"{name}_{u}" in spec for u in ("gibps", "gbps", "gbit"))


def target_point(t: dict) -> dict:
    """대상 사양의 사용률 — storage 디스크 읽기, 그리고 링크 속도가 있으면 클라이언트 링크."""
    nodes = t["storage_nodes"]
    fleet = (t["clients"] * to_gibps(t, "per_client_kv") if "clients" in t and has(t, "per_client_kv")
             else to_gibps(t, "aggregate_kv"))
    cap = to_gibps(t, "per_node_capability") if has(t, "per_node_capability") else to_gibps(t, "aggregate_capability") / nodes
    out = {"disk_read": fleet / nodes / cap}
    if "clients" in t and has(t, "per_client_nic"):
        out["client_link"] = fleet / (t["clients"] * to_gibps(t, "per_client_nic"))
    elif has(t, "aggregate_client_nic"):
        out["client_link"] = fleet / to_gibps(t, "aggregate_client_nic")
    return out


def at(lv: dict, x: float, get) -> float | None:
    """곡선에서 디스크 읽기 사용률 x 자리의 값 (이웃 두 단계 사이 직선). 범위 밖이면 None."""
    pts = sorted((s["util"]["disk_read"], get(s)) for s in lv.values())
    for (x0, y0), (x1, y1) in zip(pts, pts[1:]):
        if x0 <= x <= x1:
            return y0 + (y1 - y0) * (x - x0) / (x1 - x0) if x1 > x0 else y0
    return None


def show_target(lv: dict, t: dict) -> None:
    p = target_point(t)
    xs = [s["util"]["disk_read"] for s in lv.values()]
    print(f"\n대상 {t.get('name', '(이름 없음)')} — storage 디스크 읽기 사용률 {p['disk_read']:.3f} "
          f"(우리 곡선 {min(xs):.3f}~{max(xs):.3f})")
    for label, get in (("request 당 load p50", lambda s: s["load_s_per_request"]["p50"]),
                       ("request 당 load p99", lambda s: s["load_s_per_request"]["p99"]),
                       ("디스크 쓰기 사용률", lambda s: s["util"]["disk_write"])):
        v = at(lv, p["disk_read"], get)
        print(f"  {label:18} " + (f"{v:.3f}" if v is not None else "곡선 범위 밖 — 이 클러스터로 낼 수 없는 부하"))
    ours = statistics.median(s["util"]["client_link"] / s["util"]["disk_read"] for s in lv.values())
    if "client_link" in p:
        theirs = p["client_link"] / p["disk_read"]
        r = theirs / ours
        note = ("대상은 클라이언트 링크가 디스크보다 먼저 찬다 — 먼저 차는 자원이 우리와 다르다" if theirs > 1 >= ours else
                "비슷하다" if 0.5 < r < 2 else
                "대상은 클라이언트 링크가 우리보다 " + ("일찍 찬다" if r >= 2 else "여유 있다"))
        print(f"  클라이언트 링크 ÷ 디스크 읽기  대상 {theirs:.2f}  우리 {ours:.2f} — {note}")
    else:
        print(f"  대상 사양에 클라이언트 링크 속도가 없어 먼저 차는 자원을 비교하지 못했다 (우리 클라이언트 링크 ÷ 디스크 읽기 {ours:.2f})")


def records(run: Path) -> dict:
    s = json.loads((run / "summary.json").read_text())
    out = {}
    for h in s["hosts"]:
        for r in json.loads((run / f"driver-{h}.json").read_text())["records"]:
            if r["want"]:
                out[(h, round(r["due"], 3))] = r
    return out


def compare(a_dir: Path, b_dir: Path, metric: str = "load_s") -> dict:
    """같은 plan 을 돌린 두 결과를 request 끼리 짝지어 비교한다 (b ÷ a)."""
    a, b = records(a_dir), records(b_dir)
    keys = sorted(set(a) & set(b))
    ratio = [b[k][metric] / a[k][metric] for k in keys if a[k][metric] > 0]
    rnd = random.Random(0)             # request 를 다시 뽑아(부트스트랩) 중앙값 비율의 95% 구간
    boots = sorted(statistics.median(rnd.choices(ratio, k=len(ratio))) for _ in range(1000))
    return {"pairs": len(keys), "median_ratio": statistics.median(ratio), "ci95": [boots[25], boots[974]]}


def run_dir(run: Path, name: str) -> Path:
    return run if (run / "summary.json").exists() else run / name


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run", help="replay.py --out 폴더 (sweep 이면 단계별 폴더를 담은 것)")
    ap.add_argument("--target", help="대상 클러스터 사양 JSON")
    ap.add_argument("--baseline", help="비교할 기준 실행 (같은 plan)")
    ap.add_argument("--noise", help="기준을 한 번 더 돌린 실행 — 기준과의 차이를 잡음으로 본다")
    ap.add_argument("--out")
    args = ap.parse_args()

    run = Path(args.run)
    lv = levels(run)
    curve(lv)
    res = {"levels": {k: {"util": s["util"], "load_s_per_request": s["load_s_per_request"], "late_s": s["late_s"],
                          "nic_drops": drops(s)} for k, s in lv.items()}}
    if args.target:
        t = json.loads(Path(args.target).read_text())
        show_target(lv, t)
        res["target"] = target_point(t)
    if args.baseline:
        base = Path(args.baseline)
        print(f"\n비교 {base.name} → {run.name} (request 당 load 시간, 요청별 비율 중앙값)")
        res["compare"] = {}
        for name in lv:
            if not (run_dir(base, name) / "summary.json").exists():
                continue
            eff = compare(run_dir(base, name), run_dir(run, name))
            line = f"  {name:>9}  {eff['median_ratio']:.3f} (95% {eff['ci95'][0]:.3f}~{eff['ci95'][1]:.3f}, {eff['pairs']}쌍)"
            if args.noise and (run_dir(Path(args.noise), name) / "summary.json").exists():
                noise = compare(run_dir(base, name), run_dir(Path(args.noise), name))
                sep = eff["ci95"][0] > noise["ci95"][1] or eff["ci95"][1] < noise["ci95"][0]
                line += (f"  잡음 {noise['median_ratio']:.3f} ({noise['ci95'][0]:.3f}~{noise['ci95'][1]:.3f}) — "
                         + ("잡음 밖, 구분된다" if sep else "잡음과 겹친다"))
                eff["noise"], eff["separated"] = noise, sep
            print(line)
            res["compare"][name] = eff
    if args.out:
        Path(args.out).write_text(json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
