"""Admission: scoped HMAC lane tokens and per-key token buckets.

Token format (all ASCII, dot separated):

    mg1.<key_id>.<lane>.<expires_unix>.<sig>
    sig = base64url(HMAC-SHA256(secret, "mg1|key_id|lane|expires")) without padding

The lane claim is inside the MAC, so editing "standard" into "priority" breaks
the signature. On top of that, a priority claim is only honoured while the key
is in `priority_scope`; revoking a key from the scope downgrades it to the
standard lane instead of locking it out, so the global limits are never relaxed
for anyone who is not explicitly scoped.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional, Tuple

from .base import LANES, PRIORITY, STANDARD, GatewayError

VERSION = "mg1"


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _mac(secret: bytes, key_id: str, lane: str, exp: int) -> str:
    msg = f"{VERSION}|{key_id}|{lane}|{exp}".encode()
    return _b64(hmac.new(secret, msg, hashlib.sha256).digest())


@dataclass(frozen=True)
class Identity:
    key_id: str
    lane: str
    downgraded: bool = False


class Signer:
    """Issues and verifies lane tokens. Keep `secret` server side only."""

    def __init__(
        self,
        secret: bytes,
        priority_scope: Iterable[str] = (),
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if len(secret) < 16:
            raise ValueError("secret must be at least 16 bytes")
        self._secret = secret
        self.priority_scope = set(priority_scope)
        self._now = wall_clock

    def issue(self, key_id: str, lane: str = STANDARD, ttl: int = 3600) -> str:
        if lane not in LANES:
            raise ValueError(f"unknown lane {lane}")
        if "." in key_id or not key_id:
            raise ValueError("key_id must be non-empty and contain no dots")
        if lane == PRIORITY and key_id not in self.priority_scope:
            raise PermissionError(f"{key_id} is not in the priority scope")
        exp = int(self._now()) + ttl
        return f"{VERSION}.{key_id}.{lane}.{exp}.{_mac(self._secret, key_id, lane, exp)}"

    def verify(self, token: str) -> Identity:
        parts = token.split(".") if isinstance(token, str) else []
        if len(parts) != 5 or parts[0] != VERSION:
            raise GatewayError("local_auth", "malformed token")
        _, key_id, lane, exp_s, sig = parts
        if lane not in LANES or not exp_s.isdigit():
            raise GatewayError("local_auth", "malformed token claims")
        exp = int(exp_s)
        if not hmac.compare_digest(sig, _mac(self._secret, key_id, lane, exp)):
            raise GatewayError("local_auth", "bad signature (tampered claim or foreign secret)")
        if exp < self._now():
            raise GatewayError("local_auth", "token expired")
        if lane == PRIORITY and key_id not in self.priority_scope:
            return Identity(key_id, STANDARD, downgraded=True)
        return Identity(key_id, lane)


class TokenBucket:
    def __init__(self, rate: float, burst: float, clock: Callable[[], float] = time.monotonic) -> None:
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self._clock = clock
        self._last = clock()

    def take(self, n: float = 1.0) -> Tuple[bool, float]:
        """Returns (allowed, seconds_until_allowed)."""
        now = self._clock()
        self.tokens = min(self.burst, self.tokens + (now - self._last) * self.rate)
        self._last = now
        if self.tokens >= n:
            self.tokens -= n
            return True, 0.0
        return False, (n - self.tokens) / self.rate if self.rate > 0 else float("inf")


class KeyLimiter:
    """One bucket per key. `None` limits mean unlimited for that key."""

    def __init__(
        self,
        default: Optional[Tuple[float, float]] = (50.0, 50.0),
        overrides: Optional[Dict[str, Optional[Tuple[float, float]]]] = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.default = default
        self.overrides = dict(overrides or {})
        self._clock = clock
        self._buckets: Dict[str, TokenBucket] = {}

    def check(self, key_id: str) -> None:
        limits = self.overrides.get(key_id, self.default)
        if limits is None:
            return
        bucket = self._buckets.get(key_id)
        if bucket is None:
            bucket = self._buckets[key_id] = TokenBucket(limits[0], limits[1], self._clock)
        ok, wait = bucket.take()
        if not ok:
            raise GatewayError("local_rate_limit", f"key {key_id} over {limits[0]}/s", retry_after=wait)
