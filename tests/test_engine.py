import asyncio

import pytest

from minigate_lab import (
    PRIORITY, STANDARD, GatewayError, Gateway, KeyLimiter, MockUpstream, Pool, Profile, Scheduler, Signer,
)
from minigate_lab.cli import main
from minigate_lab.sim import BenchConfig, arrivals, bench, percentile, SCENARIOS

SECRET = b"unit-test-secret-0123456789"


def run(coro):
    return asyncio.run(coro)


# ---- scheduler ------------------------------------------------------------
async def _order(mode: str):
    sched = Scheduler(slots=1, mode=mode, weight=8, reserved=0)
    await sched.acquire(STANDARD, 1)
    order = []

    async def worker(lane, tag):
        await sched.acquire(lane, 1)
        order.append(tag)
        sched.release()

    tasks = [asyncio.ensure_future(worker(STANDARD, f"s{i}")) for i in range(3)]
    await asyncio.sleep(0)
    tasks.append(asyncio.ensure_future(worker(PRIORITY, "me")))
    await asyncio.sleep(0)
    sched.release()
    await asyncio.gather(*tasks)
    return order


def test_fifo_makes_me_wait_behind_customers_lanes_do_not():
    assert run(_order("fifo"))[-1] == "me"
    assert run(_order("lanes"))[0] == "me"


def test_reserved_slot_and_queue_timeout():
    async def go():
        sched = Scheduler(slots=2, mode="lanes", reserved=1)
        await sched.acquire(STANDARD, 1)
        with pytest.raises(GatewayError) as e:
            await sched.acquire(STANDARD, 0.05)
        assert e.value.code == "local_queue_timeout"
        await asyncio.wait_for(sched.acquire(PRIORITY, 0.05), 1)  # reserved slot is mine
        assert sched.waiting() == 0
    run(go())


def test_weighted_lanes_do_not_starve_standard():
    async def go():
        sched = Scheduler(slots=1, mode="lanes", weight=2, reserved=0)
        await sched.acquire(STANDARD, 1)
        order = []

        async def worker(lane, tag):
            await sched.acquire(lane, 2)
            order.append(tag)
            await asyncio.sleep(0)
            sched.release()

        tasks = [asyncio.ensure_future(worker(PRIORITY, f"p{i}")) for i in range(5)]
        tasks.append(asyncio.ensure_future(worker(STANDARD, "s")))
        await asyncio.sleep(0)
        sched.release()
        await asyncio.gather(*tasks)
        return order
    order = run(go())
    assert order.index("s") <= 2


# ---- gateway end to end ----------------------------------------------------
def _gateway(profiles, scope="account", attempts=3):
    signer = Signer(SECRET, priority_scope={"me"})
    pool = Pool(scope=scope, cooldown=5.0)
    for name in profiles:
        pool.add(name)
    up = MockUpstream(profiles, seed=3)
    gw = Gateway(signer, KeyLimiter(default=None), Scheduler(4, "lanes"), pool, up,
                 max_attempts=attempts, attempt_timeout=0.2)
    return gw, signer


def test_failover_skips_broken_account_and_reports_success():
    gw, signer = _gateway({"dead": Profile(latency=0.001, jitter=0, p5xx=1.0),
                           "ok": Profile(latency=0.001, jitter=0)})
    r = run(gw.handle(signer.issue("me", PRIORITY), "hi"))
    assert r.ok and r.accounts == [1, 2] and r.lane == PRIORITY


def test_gateway_blames_upstream_timeout_and_local_auth():
    gw, signer = _gateway({"slow": Profile(latency=1.0, jitter=0)}, attempts=1)
    r = run(gw.handle(signer.issue("cust-1")))
    assert (r.ok, r.error, r.blame) == (False, "upstream_timeout", "upstream")
    r = run(gw.handle("garbage"))
    assert (r.error, r.blame) == ("local_auth", "local")


def test_gateway_pool_cooldown_is_local_with_upstream_root_cause():
    gw, signer = _gateway({"a": Profile(latency=0.001, jitter=0, p429=1.0)}, scope="pool", attempts=1)
    tok = signer.issue("me", PRIORITY)
    first, second = run(gw.handle(tok)), run(gw.handle(tok))
    assert first.blame == "upstream" and first.error == "upstream_429"
    assert second.cooldown_hit and second.blame == "local" and second.root_cause == "upstream_429@acct#1"
    assert "triggered by upstream_429" in second.explain()


# ---- bench & cli ----------------------------------------------------------
def test_arrivals_are_seeded_and_percentile():
    cfg = BenchConfig(duration=1.0, seed=5)
    assert arrivals(SCENARIOS["mixed"], cfg) == arrivals(SCENARIOS["mixed"], cfg)
    assert percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 99) == 10
    assert percentile([1, 2, 3, 4], 50) == 2


def test_bench_smoke():
    rows = run(bench("calm", ["fifo+pool", "lanes+account"], BenchConfig(duration=0.4)))
    assert [r["strategy"] for r in rows] == ["fifo+pool", "lanes+account"]
    assert rows[1]["pool_cooldowns"] == 0 and rows[1]["self"]["cooldown_rate"] == 0
    assert all(r["self"]["n"] > 0 for r in rows)


def test_cli_explain_and_token(capsys):
    assert main(["explain"]) == 0
    out = capsys.readouterr().out
    assert "local_pool_cooldown" in out and "triggered by upstream_429" in out
    assert main(["token", "--secret", "x" * 20, "--key", "me", "--lane", "priority", "--scope", "me"]) == 0
    tok = capsys.readouterr().out.strip()
    assert main(["token", "--secret", "x" * 20, "--scope", "me", "--verify", tok]) == 0
    assert "lane=priority" in capsys.readouterr().out
