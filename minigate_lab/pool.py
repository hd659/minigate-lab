"""Upstream account pool with two breaker scopes.

scope="pool"    -> the naive behaviour: any upstream 429/529 freezes the WHOLE pool
                   for the cooldown, so one bad account takes everyone down.
scope="account" -> per-account breaker (closed -> open -> half_open -> closed),
                   429 honours retry_after on that account only, verification
                   demands disable just that account, healthy accounts keep serving.

Account ids are permanently monotonic: removing #2 never turns #3 into #2.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional

from .base import GatewayError

CLOSED, OPEN, HALF_OPEN, DISABLED = "closed", "open", "half_open", "disabled"


@dataclass
class Account:
    id: int
    name: str
    state: str = CLOSED
    fails: int = 0
    open_until: float = 0.0
    trips: int = 0
    inflight: int = 0
    served: int = 0
    probing: bool = False
    disabled_reason: str = ""
    last_cause: str = ""
    last_cause_at: float = 0.0


class Pool:
    def __init__(
        self,
        scope: str = "account",
        fail_threshold: int = 3,
        cooldown: float = 1.0,
        max_cooldown: float = 8.0,
        account_cap: Optional[int] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if scope not in ("pool", "account"):
            raise ValueError("scope must be 'pool' or 'account'")
        self.scope = scope
        self.fail_threshold = fail_threshold
        self.cooldown = cooldown
        self.max_cooldown = max_cooldown
        self.account_cap = account_cap  # gateway-side per-account concurrency cap
        self._clock = clock
        self._next_id = 1
        self.accounts: Dict[int, Account] = {}
        self.pool_cooldown_until = 0.0
        self.pool_cooldown_cause: Optional[str] = None
        self.events: List[tuple] = []

    # ---- membership -------------------------------------------------------
    def add(self, name: str) -> Account:
        acct = Account(self._next_id, name)
        self._next_id += 1
        self.accounts[acct.id] = acct
        return acct

    def remove(self, acct_id: int) -> None:
        self.accounts.pop(acct_id, None)

    def ids(self) -> List[int]:
        return sorted(self.accounts)

    # ---- selection --------------------------------------------------------
    def _refresh(self, acct: Account, now: float) -> None:
        if acct.state == OPEN and now >= acct.open_until:
            acct.state = HALF_OPEN
            acct.probing = False

    def pick(self, exclude: Iterable[int] = ()) -> Account:
        now = self._clock()
        if self.scope == "pool" and now < self.pool_cooldown_until:
            raise GatewayError(
                "local_pool_cooldown",
                root_cause=self.pool_cooldown_cause,
                retry_after=self.pool_cooldown_until - now,
            )
        skip = set(exclude)
        candidates = []
        for acct in self.accounts.values():
            self._refresh(acct, now)
            if acct.id in skip or acct.state in (OPEN, DISABLED):
                continue
            if acct.state == HALF_OPEN and acct.probing:
                continue  # exactly one probe at a time
            if self.account_cap is not None and acct.inflight >= self.account_cap:
                continue  # never push an account past its known quota ourselves
            candidates.append(acct)
        if not candidates:
            blocked = [a for a in self.accounts.values() if a.id not in skip and a.last_cause]
            latest = max(blocked, key=lambda a: a.last_cause_at, default=None)
            raise GatewayError("local_no_account", detail=self._why_empty(skip),
                               root_cause=latest.last_cause if latest else None)
        # Prefer fully closed accounts, then least loaded, then lowest id.
        acct = min(candidates, key=lambda a: (a.state != CLOSED, a.inflight, a.id))
        if acct.state == HALF_OPEN:
            acct.probing = True
        acct.inflight += 1
        return acct

    def _why_empty(self, skip: set) -> str:
        states = {a.id: ("tried" if a.id in skip else a.state) for a in self.accounts.values()}
        return "no usable account: " + ", ".join(f"#{k}={v}" for k, v in sorted(states.items()))

    # ---- feedback ---------------------------------------------------------
    def release(self, acct: Account) -> None:
        acct.inflight = max(0, acct.inflight - 1)

    def report_success(self, acct: Account) -> None:
        self.release(acct)
        acct.served += 1
        acct.fails = 0
        if acct.state == HALF_OPEN:
            acct.state = CLOSED
            acct.probing = False
            acct.trips = 0
            self.events.append((self._clock(), acct.id, "closed"))

    def report_failure(self, acct: Account, err: GatewayError) -> None:
        self.release(acct)
        now = self._clock()
        cause = f"{err.code}@acct#{acct.id}"
        acct.last_cause, acct.last_cause_at = cause, now
        if err.code == "upstream_verify":
            acct.state = DISABLED
            acct.disabled_reason = err.detail
            self.events.append((now, acct.id, "disabled"))
            return
        if self.scope == "pool":
            if err.code in ("upstream_429", "upstream_529"):
                wait = err.retry_after or self.cooldown
                if now + wait > self.pool_cooldown_until:
                    self.pool_cooldown_until = now + wait
                    self.pool_cooldown_cause = cause
                    self.events.append((now, acct.id, "pool_cooldown"))
            return
        # per-account breaker
        if err.code == "upstream_429":
            # Rate limit is a polite "not now": rest this account only, for as long as asked.
            self._open(acct, now, err.retry_after or self.cooldown)
            return
        acct.fails += 1
        if acct.state == HALF_OPEN or acct.fails >= self.fail_threshold:
            backoff = min(self.max_cooldown, self.cooldown * (2 ** acct.trips))
            acct.trips += 1
            self._open(acct, now, backoff)

    def _open(self, acct: Account, now: float, duration: float) -> None:
        acct.state = OPEN
        acct.probing = False
        acct.fails = 0
        acct.open_until = max(acct.open_until, now + duration)
        self.events.append((now, acct.id, "open"))

    def snapshot(self) -> List[dict]:
        now = self._clock()
        out = []
        for acct in sorted(self.accounts.values(), key=lambda a: a.id):
            self._refresh(acct, now)
            out.append({"id": acct.id, "name": acct.name, "state": acct.state,
                        "served": acct.served, "trips": acct.trips})
        return out
