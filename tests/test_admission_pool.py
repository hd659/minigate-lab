import pytest

from minigate_lab import (
    PRIORITY, STANDARD, Blame, GatewayError, KeyLimiter, Pool, Signer, error_from_status,
)

SECRET = b"unit-test-secret-0123456789"


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t


# ---- signed lanes ---------------------------------------------------------
def test_token_roundtrip_and_tamper():
    s = Signer(SECRET, priority_scope={"me"})
    tok = s.issue("me", PRIORITY)
    assert s.verify(tok).lane == PRIORITY
    cust = s.issue("cust-1", STANDARD)
    forged = cust.replace(".standard.", ".priority.")
    with pytest.raises(GatewayError) as e:
        s.verify(forged)
    assert e.value.code == "local_auth" and e.value.blame is Blame.LOCAL
    with pytest.raises(GatewayError):
        Signer(b"another-secret-abcdefgh").verify(tok)
    with pytest.raises(PermissionError):
        s.issue("cust-1", PRIORITY)


def test_scope_revocation_downgrades_instead_of_locking_out():
    s = Signer(SECRET, priority_scope={"me"})
    tok = s.issue("me", PRIORITY)
    s.priority_scope.discard("me")
    ident = s.verify(tok)
    assert ident.lane == STANDARD and ident.downgraded


def test_token_expiry():
    clock = FakeClock()
    s = Signer(SECRET, wall_clock=clock)
    tok = s.issue("cust-1", ttl=10)
    clock.t += 11
    with pytest.raises(GatewayError, match="expired"):
        s.verify(tok)


def test_key_limiter_is_per_key():
    clock = FakeClock()
    lim = KeyLimiter(default=(1.0, 2.0), overrides={"me": None}, clock=clock)
    lim.check("cust-1")
    lim.check("cust-1")
    with pytest.raises(GatewayError) as e:
        lim.check("cust-1")
    assert e.value.code == "local_rate_limit" and e.value.retry_after == pytest.approx(1.0)
    lim.check("cust-2")  # other customers unaffected
    for _ in range(100):
        lim.check("me")  # unlimited override
    clock.t += 1.0
    lim.check("cust-1")


# ---- pool -----------------------------------------------------------------
def test_account_ids_are_monotonic():
    pool = Pool()
    a, b, c = (pool.add(n) for n in "abc")
    pool.remove(b.id)
    d = pool.add("d")
    assert (a.id, c.id, d.id) == (1, 3, 4)
    assert pool.ids() == [1, 3, 4]


def test_pool_scope_one_429_freezes_everyone():
    clock = FakeClock()
    pool = Pool(scope="pool", clock=clock)
    a, _ = pool.add("a"), pool.add("b")
    pool.report_failure(pool.pick(), GatewayError("upstream_429", retry_after=2.0))
    with pytest.raises(GatewayError) as e:
        pool.pick()
    assert e.value.code == "local_pool_cooldown" and e.value.root_cause == f"upstream_429@acct#{a.id}"
    clock.t += 2.1
    assert pool.pick().state == "closed"


def test_account_scope_isolates_the_bad_account_and_half_opens():
    clock = FakeClock()
    pool = Pool(scope="account", fail_threshold=2, cooldown=1.0, clock=clock)
    bad, good = pool.add("bad"), pool.add("good")
    for _ in range(2):
        acct = pool.pick(exclude=[good.id])
        pool.report_failure(acct, GatewayError("upstream_5xx"))
    assert bad.state == "open"
    assert pool.pick().id == good.id  # healthy account keeps serving
    pool.release(good)
    clock.t += 1.01
    probe = pool.pick(exclude=[good.id])
    assert probe.id == bad.id and bad.state == "half_open"
    with pytest.raises(GatewayError):
        pool.pick(exclude=[good.id])  # only one probe in flight
    pool.report_failure(probe, GatewayError("upstream_5xx"))
    assert bad.state == "open" and bad.open_until == pytest.approx(clock.t + 2.0)  # backoff doubled
    clock.t += 2.01
    pool.report_success(pool.pick(exclude=[good.id]))
    assert bad.state == "closed" and bad.trips == 0


def test_account_cap_keeps_us_under_known_quota():
    pool = Pool(scope="account", account_cap=1)
    a, b = pool.add("a"), pool.add("b")
    assert [pool.pick().id, pool.pick().id] == [a.id, b.id]
    with pytest.raises(GatewayError) as e:
        pool.pick()
    assert e.value.code == "local_no_account"


def test_verify_demand_disables_only_that_account():
    pool = Pool(scope="pool")
    a, b = pool.add("a"), pool.add("b")
    pool.report_failure(a, GatewayError("upstream_verify"))
    assert a.state == "disabled"
    assert pool.pick().id == b.id


def test_no_account_error_names_the_upstream_trigger():
    pool = Pool(scope="account")
    a = pool.add("a")
    pool.report_failure(pool.pick(), GatewayError("upstream_429", retry_after=5))
    with pytest.raises(GatewayError) as e:
        pool.pick()
    assert e.value.code == "local_no_account" and e.value.blame is Blame.LOCAL
    assert e.value.root_cause == f"upstream_429@acct#{a.id}"


def test_status_mapping():
    assert error_from_status(200) is None
    assert error_from_status(429, 3).code == "upstream_429"
    assert error_from_status(529).code == "upstream_529"
    assert error_from_status(403, body="please verify your identity").code == "upstream_verify"
    assert error_from_status(502).blame is Blame.UPSTREAM
    with pytest.raises(ValueError):
        GatewayError("made_up_code")
