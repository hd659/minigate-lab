"""Fault-injecting mock upstream.

Each account has its own profile: base latency, jitter, slow-tail probability,
random 429/529/5xx rates, a per-account concurrency quota (exceeding it returns
429 with retry_after, like a real provider), and an optional time at which the
provider starts demanding verification. Everything is seeded, so two strategies
in a bench see the same fault dice per call index.
"""
from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from .base import GatewayError
from .pool import Account


@dataclass
class Profile:
    latency: float = 0.05
    jitter: float = 0.02
    slow_p: float = 0.0
    slow_factor: float = 6.0
    p429: float = 0.0
    p529: float = 0.0
    p5xx: float = 0.0
    retry_after: float = 0.5
    quota: Optional[int] = None
    verify_after: Optional[float] = None


class MockUpstream:
    def __init__(
        self,
        profiles: Optional[Dict[str, Profile]] = None,
        default: Optional[Profile] = None,
        seed: int = 7,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
    ) -> None:
        self.profiles = dict(profiles or {})
        self.default = default or Profile()
        self.rng = random.Random(seed)
        self._clock = clock
        self._sleep = sleep
        self._t0 = clock()
        self.inflight: Counter = Counter()
        self.calls: Counter = Counter()
        self.outcomes: Counter = Counter()

    def profile(self, name: str) -> Profile:
        return self.profiles.get(name, self.default)

    async def call(self, account: Account, payload: Any = None) -> str:
        prof = self.profile(account.name)
        self.calls[account.name] += 1
        elapsed = self._clock() - self._t0
        roll = self.rng.random()
        lat = max(0.001, prof.latency + self.rng.uniform(-prof.jitter, prof.jitter))
        if self.rng.random() < prof.slow_p:
            lat *= prof.slow_factor

        if prof.verify_after is not None and elapsed >= prof.verify_after:
            await self._sleep(0.005)
            self._mark(account, "403_verify")
            raise GatewayError("upstream_verify", f"{account.name}: provider requires identity verification")
        if prof.quota is not None and self.inflight[account.name] >= prof.quota:
            await self._sleep(0.003)
            self._mark(account, "429_quota")
            raise GatewayError("upstream_429", f"{account.name}: concurrency quota", retry_after=prof.retry_after)
        if roll < prof.p429:
            await self._sleep(0.005)
            self._mark(account, "429_random")
            raise GatewayError("upstream_429", f"{account.name}: rate limited", retry_after=prof.retry_after)
        if roll < prof.p429 + prof.p529:
            await self._sleep(lat * 0.5)
            self._mark(account, "529")
            raise GatewayError("upstream_529", f"{account.name}: overloaded", retry_after=prof.retry_after)
        if roll < prof.p429 + prof.p529 + prof.p5xx:
            await self._sleep(lat * 0.3)
            self._mark(account, "500")
            raise GatewayError("upstream_5xx", f"{account.name}: internal error")

        self.inflight[account.name] += 1
        try:
            await self._sleep(lat)
        finally:
            self.inflight[account.name] -= 1
        self._mark(account, "200")
        return f"echo:{account.name}:{payload}"

    def _mark(self, account: Account, outcome: str) -> None:
        self.outcomes[(account.name, outcome)] += 1
