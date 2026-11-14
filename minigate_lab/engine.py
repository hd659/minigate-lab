"""Gateway engine: slot scheduler + request pipeline with failover and blame.

Pipeline for one request:
    verify token (HMAC, scope)      -> local_auth
    per-key token bucket            -> local_rate_limit
    wait for a slot (fifo | lanes)  -> local_queue_timeout
    pick account, call upstream     -> upstream_* or local_pool_cooldown / local_no_account
    failover to another account up to max_attempts
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from typing import Any, Awaitable, Callable, Optional, Protocol

from .admission import KeyLimiter, Signer
from .base import PRIORITY, STANDARD, GatewayError, Result
from .pool import Account, Pool


class Upstream(Protocol):
    def call(self, account: Account, payload: Any) -> Awaitable[Any]: ...


class Scheduler:
    """Concurrency slots in front of the upstream pool.

    mode="fifo":  one global queue, lane is ignored (everyone waits together).
    mode="lanes": two queues. When both are backlogged, up to `weight` priority
                  waiters are served per standard waiter (no starvation), and
                  `reserved` slots can only be taken by the priority lane.
    """

    def __init__(self, slots: int, mode: str = "lanes", weight: int = 8, reserved: int = 1) -> None:
        if mode not in ("fifo", "lanes"):
            raise ValueError("mode must be 'fifo' or 'lanes'")
        if mode == "lanes" and not 0 <= reserved < slots:
            raise ValueError("reserved must be in [0, slots)")
        self.slots = slots
        self.mode = mode
        self.weight = max(1, weight)
        self.reserved = reserved if mode == "lanes" else 0
        self.in_use = 0
        self._credit = self.weight
        self._q = {PRIORITY: deque(), STANDARD: deque()}

    def _queue(self, lane: str) -> deque:
        return self._q[STANDARD] if self.mode == "fifo" else self._q[lane]

    def _cap(self, lane: str) -> int:
        return self.slots if lane == PRIORITY else self.slots - self.reserved

    def waiting(self) -> int:
        return len(self._q[PRIORITY]) + len(self._q[STANDARD])

    def _next(self) -> Optional[deque]:
        if self.mode == "fifo":
            q = self._q[STANDARD]
            return q if q and self.in_use < self.slots else None
        p_ok = bool(self._q[PRIORITY]) and self.in_use < self._cap(PRIORITY)
        s_ok = bool(self._q[STANDARD]) and self.in_use < self._cap(STANDARD)
        if p_ok and s_ok:
            if self._credit > 0:
                self._credit -= 1
                return self._q[PRIORITY]
            self._credit = self.weight
            return self._q[STANDARD]
        if p_ok:
            return self._q[PRIORITY]
        if s_ok:
            self._credit = self.weight
            return self._q[STANDARD]
        return None

    def _dispatch(self) -> None:
        while True:
            q = self._next()
            if q is None:
                return
            fut = q.popleft()
            if fut.done():  # cancelled waiter
                continue
            self.in_use += 1
            fut.set_result(None)

    async def acquire(self, lane: str, timeout: float) -> None:
        q = self._queue(lane)
        if not q and self.in_use < self._cap(lane):
            self.in_use += 1
            return
        fut = asyncio.get_running_loop().create_future()
        q.append(fut)
        try:
            done, _ = await asyncio.wait({fut}, timeout=timeout)
        except asyncio.CancelledError:
            # Caller went away: give back a slot we were granted, or leave the queue.
            if fut.done() and not fut.cancelled():
                self.release()
            else:
                fut.cancel()
            raise
        if not done:
            fut.cancel()
            try:
                q.remove(fut)
            except ValueError:
                pass
            raise GatewayError("local_queue_timeout", f"no {lane} slot within {timeout:.2f}s")

    def release(self) -> None:
        self.in_use -= 1
        self._dispatch()


class Gateway:
    def __init__(
        self,
        signer: Signer,
        limiter: KeyLimiter,
        scheduler: Scheduler,
        pool: Pool,
        upstream: Upstream,
        max_attempts: int = 3,
        queue_timeout: float = 2.0,
        attempt_timeout: float = 2.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.signer = signer
        self.limiter = limiter
        self.scheduler = scheduler
        self.pool = pool
        self.upstream = upstream
        self.max_attempts = max_attempts
        self.queue_timeout = queue_timeout
        self.attempt_timeout = attempt_timeout
        self._clock = clock

    @staticmethod
    def _fail(res: Result, err: GatewayError) -> Result:
        res.ok = False
        res.error = err.code
        res.blame = err.blame.value
        res.root_cause = err.root_cause
        return res

    async def handle(self, token: str, payload: Any = None) -> Result:
        t0 = self._clock()
        res = Result(key_id="?")
        try:
            ident = self.signer.verify(token)
            res.key_id, res.lane, res.downgraded = ident.key_id, ident.lane, ident.downgraded
            self.limiter.check(ident.key_id)
            await self.scheduler.acquire(ident.lane, self.queue_timeout)
        except GatewayError as err:
            res.queue_wait = self._clock() - t0
            res.latency = res.queue_wait
            return self._fail(res, err)
        res.queue_wait = self._clock() - t0
        try:
            await self._dispatch(res, payload)
        finally:
            self.scheduler.release()
            res.latency = self._clock() - t0
        return res

    async def _dispatch(self, res: Result, payload: Any) -> None:
        last: Optional[GatewayError] = None
        while res.attempts < self.max_attempts:
            try:
                acct = self.pool.pick(exclude=res.accounts)
            except GatewayError as err:
                if last is not None and err.root_cause is None:
                    err.root_cause = f"{last.code}@acct#{res.accounts[-1]}"
                self._fail(res, err)
                return
            res.attempts += 1
            res.accounts.append(acct.id)
            try:
                await asyncio.wait_for(self.upstream.call(acct, payload), self.attempt_timeout)
            except asyncio.TimeoutError:
                last = GatewayError("upstream_timeout", f"acct#{acct.id} > {self.attempt_timeout}s")
            except GatewayError as err:
                last = err
            else:
                self.pool.report_success(acct)
                res.ok, res.error, res.blame = True, None, None
                return
            self.pool.report_failure(acct, last)
        self._fail(res, last)
