"""Command line entry: `python3 -m minigate_lab <command>`.

    bench     replay one scenario against the 2x2 strategy matrix and print a table
    explain   send a handful of crafted requests and print who refused each one and why
    token     issue / verify a lane token with a given secret
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import List, Optional

from .admission import KeyLimiter, Signer
from .base import PRIORITY, STANDARD, GatewayError
from .engine import Gateway, Scheduler
from .pool import Pool
from .sim import SCENARIOS, SECRET, SELF_KEY, STRATEGIES, BenchConfig, bench
from .upstream import MockUpstream, Profile

COLS = [("strategy", 14), ("who", 9), ("ok%", 6), ("wait p50", 9), ("wait p99", 9),
        ("lat p99", 8), ("cooldown%", 10), ("up/local fail", 14)]


def render(rows: List[dict]) -> str:
    out = ["  ".join(name.ljust(w) for name, w in COLS)]
    for row in rows:
        for who in ("self", "customers"):
            s = row[who]
            cells = [row["strategy"] if who == "self" else "", who, f"{s['ok_rate'] * 100:.1f}",
                     f"{s['wait_p50_ms']:.0f}ms", f"{s['wait_p99_ms']:.0f}ms", f"{s['lat_p99_ms']:.0f}ms",
                     f"{s['cooldown_rate'] * 100:.1f}", f"{s['upstream_fail']}/{s['local_fail']}"]
            out.append("  ".join(c.ljust(w) for c, (_, w) in zip(cells, COLS)))
    out.append("")
    for row in rows:
        s = row["self"]
        codes = ", ".join(f"{k}={v}" for k, v in sorted(s["codes"].items())) or "none"
        causes = ", ".join(f"{k}={v}" for k, v in sorted(s["causes"].items())) or "none"
        states = " ".join(f"#{a['id']}:{a['state']}" for a in row["pool"])
        out.append(f"[{row['strategy']}] self failures: {codes} | root causes: {causes}")
        out.append(f"    pool cooldowns={row['pool_cooldowns']} breaker trips={row['breaker_trips']} final: {states}")
    return "\n".join(out)


def cmd_bench(args: argparse.Namespace) -> int:
    cfg = BenchConfig(duration=args.duration, seed=args.seed, slots=args.slots, weight=args.weight,
                      reserved=args.reserved, queue_timeout=args.queue_timeout)
    strategies = args.strategies.split(",") if args.strategies else None
    for s in strategies or []:
        if s not in STRATEGIES:
            print(f"unknown strategy {s}; choose from {', '.join(STRATEGIES)}", file=sys.stderr)
            return 2
    rows = asyncio.run(bench(args.scenario, strategies, cfg))
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        sc = SCENARIOS[args.scenario]
        print(f"scenario={sc.name}: {sc.note}  (duration={cfg.duration}s seed={cfg.seed} slots={cfg.slots})\n")
        print(render(rows))
    return 0


async def _explain() -> List[str]:
    signer = Signer(SECRET, priority_scope={SELF_KEY})
    limiter = KeyLimiter(default=(2.0, 2.0))
    pool = Pool(scope="pool", cooldown=5.0)
    pool.add("acct-a")
    up = MockUpstream({"acct-a": Profile(latency=0.01, jitter=0.0, p429=1.0)}, seed=1)
    gw = Gateway(signer, limiter, Scheduler(2, mode="lanes"), pool, up, max_attempts=1)
    good = signer.issue("cust-1", STANDARD)
    forged = good.replace(f".{STANDARD}.", f".{PRIORITY}.")
    cases = [("forged priority claim", forged), ("first call, upstream 429s", good),
             ("second call, pool frozen", good), ("third call, key over its bucket", good)]
    lines = []
    for label, tok in cases:
        r = await gw.handle(tok)
        lines.append(f"{label:<32} -> {r.explain()}")
    return lines


def cmd_explain(_: argparse.Namespace) -> int:
    for line in asyncio.run(_explain()):
        print(line)
    return 0


def cmd_token(args: argparse.Namespace) -> int:
    signer = Signer(args.secret.encode(), priority_scope=set(args.scope.split(",")) if args.scope else ())
    try:
        if args.verify:
            ident = signer.verify(args.verify)
            print(f"valid key={ident.key_id} lane={ident.lane} downgraded={ident.downgraded}")
        else:
            print(signer.issue(args.key, args.lane, args.ttl))
    except (GatewayError, PermissionError, ValueError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="minigate_lab", description="minimal LLM gateway lab")
    sub = p.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("bench", help="compare scheduling / breaker strategies")
    b.add_argument("--scenario", default="mixed", choices=sorted(SCENARIOS))
    b.add_argument("--strategies", default="", help=f"comma list from {','.join(STRATEGIES)}")
    b.add_argument("--duration", type=float, default=4.0)
    b.add_argument("--seed", type=int, default=7)
    b.add_argument("--slots", type=int, default=12)
    b.add_argument("--weight", type=int, default=8)
    b.add_argument("--reserved", type=int, default=1)
    b.add_argument("--queue-timeout", type=float, default=1.5)
    b.add_argument("--json", action="store_true")
    b.set_defaults(func=cmd_bench)
    e = sub.add_parser("explain", help="show blame classification on crafted requests")
    e.set_defaults(func=cmd_explain)
    t = sub.add_parser("token", help="issue or verify a lane token")
    t.add_argument("--secret", required=True, help="at least 16 bytes")
    t.add_argument("--key", default="me")
    t.add_argument("--lane", default=STANDARD, choices=[PRIORITY, STANDARD])
    t.add_argument("--scope", default="", help="comma list of key ids allowed on the priority lane")
    t.add_argument("--ttl", type=int, default=3600)
    t.add_argument("--verify", default="", help="token to verify instead of issuing")
    t.set_defaults(func=cmd_token)
    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
