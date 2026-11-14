"""Bench: replay one seeded open-loop arrival schedule against several strategies.

Strategies form a 2x2 so each change can be credited separately:
    scheduler  fifo | lanes       (global queue vs HMAC-scoped priority lane)
    breaker    pool | account     (whole-pool cooldown vs per-account breaker)
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from dataclasses import dataclass, field, replace
from typing import Dict, List, Optional

from .admission import KeyLimiter, Signer
from .base import PRIORITY, STANDARD, Result
from .engine import Gateway, Scheduler
from .pool import Pool
from .upstream import MockUpstream, Profile

SELF_KEY = "me"
SECRET = b"minigate-lab-demo-secret-not-for-prod"

STRATEGIES = {
    "fifo+pool": ("fifo", "pool"),
    "fifo+account": ("fifo", "account"),
    "lanes+pool": ("lanes", "pool"),
    "lanes+account": ("lanes", "account"),
}

BASE = Profile(latency=0.06, jitter=0.02, slow_p=0.03, p429=0.005, p529=0.003, quota=4, retry_after=0.4)


@dataclass
class Scenario:
    name: str
    accounts: Dict[str, Profile]
    customer_rps: float = 100.0
    customers: int = 4
    self_rps: float = 10.0
    burst_customer_rps: float = 0.0
    note: str = ""


def _accounts(**over: Profile) -> Dict[str, Profile]:
    accts = {f"acct-{c}": BASE for c in "abcde"}
    accts.update({k.replace("_", "-"): v for k, v in over.items()})
    return accts


SCENARIOS = {
    "calm": Scenario("calm", _accounts(), customer_rps=70.0,
                     note="healthy pool, moderate load; sanity baseline"),
    "mixed": Scenario("mixed", _accounts(acct_c=replace(BASE, p429=0.25, p5xx=0.05),
                                         acct_e=replace(BASE, verify_after=1.0)),
                      burst_customer_rps=90.0,
                      note="one flaky account, one gets a verification demand at t=1s, one abusive customer"),
    "dead-account": Scenario("dead-account", _accounts(acct_b=replace(BASE, p5xx=1.0)),
                             note="one account always returns 500"),
    "overload": Scenario("overload", _accounts(), customer_rps=240.0,
                         note="customer demand about 1.5x gateway capacity"),
}


@dataclass
class BenchConfig:
    duration: float = 4.0
    seed: int = 7
    slots: int = 12
    weight: int = 8
    reserved: int = 1
    queue_timeout: float = 1.5
    attempt_timeout: float = 1.0
    max_attempts: int = 3
    customer_limit: tuple = (60.0, 60.0)
    self_limit: tuple = (30.0, 30.0)


def arrivals(sc: Scenario, cfg: BenchConfig) -> List[tuple]:
    rng = random.Random(cfg.seed)
    streams = [(SELF_KEY, sc.self_rps)]
    streams += [(f"cust-{i + 1}", sc.customer_rps / sc.customers) for i in range(sc.customers)]
    if sc.burst_customer_rps:
        streams.append(("cust-burst", sc.burst_customer_rps))
    out = []
    for key, rate in streams:
        t = rng.expovariate(rate)
        while t < cfg.duration:
            out.append((t, key))
            t += rng.expovariate(rate)
    return sorted(out)


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    rank = max(1, min(len(s), int(-(-q * len(s) // 100))))
    return s[rank - 1]


@dataclass
class Stats:
    n: int = 0
    ok: int = 0
    wait: List[float] = field(default_factory=list)
    lat: List[float] = field(default_factory=list)
    cooldown: int = 0
    blame: Counter = field(default_factory=Counter)
    codes: Counter = field(default_factory=Counter)
    causes: Counter = field(default_factory=Counter)

    def add(self, r: Result) -> None:
        self.n += 1
        self.wait.append(r.queue_wait)
        self.cooldown += r.cooldown_hit
        if r.ok:
            self.ok += 1
            self.lat.append(r.latency)
        else:
            self.blame[r.blame] += 1
            self.codes[r.error] += 1
            if r.root_cause:
                self.causes[r.root_cause.split("@")[0]] += 1

    def summary(self) -> dict:
        n = max(1, self.n)
        return {
            "n": self.n, "ok_rate": self.ok / n,
            "wait_p50_ms": percentile(self.wait, 50) * 1e3, "wait_p99_ms": percentile(self.wait, 99) * 1e3,
            "lat_p50_ms": percentile(self.lat, 50) * 1e3, "lat_p99_ms": percentile(self.lat, 99) * 1e3,
            "cooldown_rate": self.cooldown / n,
            "upstream_fail": self.blame["upstream"], "local_fail": self.blame["local"],
            "codes": dict(self.codes), "causes": dict(self.causes),
        }


def build_gateway(strategy: str, sc: Scenario, cfg: BenchConfig, upstream: Optional[MockUpstream] = None):
    mode, scope = STRATEGIES[strategy]
    signer = Signer(SECRET, priority_scope={SELF_KEY})
    limiter = KeyLimiter(default=cfg.customer_limit, overrides={SELF_KEY: cfg.self_limit})
    sched = Scheduler(cfg.slots, mode=mode, weight=cfg.weight, reserved=cfg.reserved)
    # Both breaker scopes get the same per-account cap so only the scope differs.
    pool = Pool(scope=scope, cooldown=0.4, fail_threshold=3, account_cap=BASE.quota)
    for name in sc.accounts:
        pool.add(name)
    up = upstream or MockUpstream(sc.accounts, default=BASE, seed=cfg.seed)
    gw = Gateway(signer, limiter, sched, pool, up, max_attempts=cfg.max_attempts,
                 queue_timeout=cfg.queue_timeout, attempt_timeout=cfg.attempt_timeout)
    return gw, signer


async def run_strategy(strategy: str, sc: Scenario, cfg: BenchConfig) -> dict:
    gw, signer = build_gateway(strategy, sc, cfg)
    sched = arrivals(sc, cfg)
    tokens = {k: signer.issue(k, PRIORITY if k == SELF_KEY else STANDARD) for k in {k for _, k in sched}}
    groups = {"self": Stats(), "customers": Stats()}
    tasks = []
    t0 = time.monotonic()
    for at, key in sched:
        delay = at - (time.monotonic() - t0)
        if delay > 0:
            await asyncio.sleep(delay)
        tasks.append(asyncio.ensure_future(gw.handle(tokens[key], payload=at)))
    for r in await asyncio.gather(*tasks):
        groups["self" if r.key_id == SELF_KEY else "customers"].add(r)
    return {
        "strategy": strategy,
        "self": groups["self"].summary(),
        "customers": groups["customers"].summary(),
        "pool": gw.pool.snapshot(),
        "pool_cooldowns": sum(1 for e in gw.pool.events if e[2] == "pool_cooldown"),
        "breaker_trips": sum(1 for e in gw.pool.events if e[2] == "open"),
    }


async def bench(scenario: str = "mixed", strategies: Optional[List[str]] = None,
                cfg: Optional[BenchConfig] = None) -> List[dict]:
    cfg = cfg or BenchConfig()
    sc = SCENARIOS[scenario]
    return [await run_strategy(s, sc, cfg) for s in (strategies or list(STRATEGIES))]
