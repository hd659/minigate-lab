"""Shared vocabulary: lanes, the blame taxonomy, typed errors and per-request results.

Every failure the gateway returns carries exactly one error code, and every code
maps to exactly one side of the fence: UPSTREAM (the provider said no) or LOCAL
(this gateway said no). When the gateway refuses because of something upstream
did earlier (e.g. a pool cooldown tripped by an upstream 429), the code stays
LOCAL and `root_cause` points at the upstream event, so "who refused" and
"what triggered it" are never conflated.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

PRIORITY = "priority"
STANDARD = "standard"
LANES = (PRIORITY, STANDARD)


class Blame(str, Enum):
    UPSTREAM = "upstream"
    LOCAL = "local"


ERROR_CODES = {
    "upstream_429": (Blame.UPSTREAM, "upstream account rate limited (429)"),
    "upstream_529": (Blame.UPSTREAM, "upstream overloaded (529/503)"),
    "upstream_5xx": (Blame.UPSTREAM, "upstream internal error (5xx)"),
    "upstream_timeout": (Blame.UPSTREAM, "upstream did not answer within the attempt timeout"),
    "upstream_verify": (Blame.UPSTREAM, "upstream demands account verification; account auto-disabled"),
    "local_auth": (Blame.LOCAL, "token malformed, tampered, or signed with another secret"),
    "local_rate_limit": (Blame.LOCAL, "this key exceeded its own token bucket"),
    "local_queue_timeout": (Blame.LOCAL, "waited too long for a gateway slot"),
    "local_pool_cooldown": (Blame.LOCAL, "gateway refused: the whole pool is in cooldown"),
    "local_no_account": (Blame.LOCAL, "gateway refused: every account is open, disabled, or already tried"),
}


class GatewayError(Exception):
    """A classified failure. `code` must be a key of ERROR_CODES."""

    def __init__(
        self,
        code: str,
        detail: str = "",
        root_cause: Optional[str] = None,
        retry_after: Optional[float] = None,
    ) -> None:
        if code not in ERROR_CODES:
            raise ValueError(f"unknown error code: {code}")
        self.code = code
        self.blame: Blame = ERROR_CODES[code][0]
        self.detail = detail or ERROR_CODES[code][1]
        self.root_cause = root_cause
        self.retry_after = retry_after
        super().__init__(f"[{self.blame.value}] {code}: {self.detail}")


def error_from_status(status: int, retry_after: Optional[float] = None, body: str = "") -> Optional[GatewayError]:
    """Map a real upstream HTTP status to a classified error (None means success).

    This is the seam where a real provider adapter plugs in; the mock upstream
    raises the same codes directly.
    """
    if 200 <= status < 300:
        return None
    if status == 429:
        return GatewayError("upstream_429", retry_after=retry_after)
    if status in (529, 503):
        return GatewayError("upstream_529", retry_after=retry_after)
    if status == 403 and "verif" in body.lower():
        return GatewayError("upstream_verify", detail=body[:120] or ERROR_CODES["upstream_verify"][1])
    if status >= 500:
        return GatewayError("upstream_5xx", detail=f"status {status}")
    # Other 4xx (bad request, auth to provider) are still the provider's verdict.
    return GatewayError("upstream_5xx", detail=f"unexpected upstream status {status}")


@dataclass
class Result:
    """Outcome of one gateway request, used both by callers and the bench."""

    key_id: str
    lane: str = STANDARD
    ok: bool = False
    queue_wait: float = 0.0
    latency: float = 0.0
    attempts: int = 0
    accounts: list = field(default_factory=list)
    error: Optional[str] = None
    blame: Optional[str] = None
    root_cause: Optional[str] = None
    downgraded: bool = False

    @property
    def cooldown_hit(self) -> bool:
        return self.error == "local_pool_cooldown"

    def explain(self) -> str:
        if self.ok:
            return f"ok via acct#{self.accounts[-1]} after {self.attempts} attempt(s)"
        why = ERROR_CODES[self.error][1] if self.error else "unknown"
        cause = f"; triggered by {self.root_cause}" if self.root_cause else ""
        return f"{self.blame} refused ({self.error}): {why}{cause}"
