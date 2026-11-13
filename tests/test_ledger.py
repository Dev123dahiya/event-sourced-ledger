import random
import sqlite3
import threading

import pytest

from ledger import (AccountExists, ConcurrencyError, Entry, EventStore, InsufficientFunds, Ledger,
                    UnbalancedTransaction, UnknownAccount)


@pytest.fixture
def ledger(tmp_path):
    led = Ledger(EventStore(str(tmp_path / "ledger.db")))
    led.open_account("cash", "Cash", normal="debit", min_balance=0)
    led.open_account("equity", "Owner equity", normal="credit")
    led.open_account("alice", "Alice", normal="debit", min_balance=0)
    led.open_account("bob", "Bob", normal="debit", min_balance=0)
    led.post([Entry("cash", debit=10_000), Entry("equity", credit=10_000)], "seed capital")
    return led


def test_balanced_transaction_moves_money(ledger):
    ledger.transfer("cash", "alice", 2_500, "payout")
    assert ledger.balance("cash") == 7_500 and ledger.balance("alice") == 2_500
    assert ledger.balance("equity") == 10_000  # credit-normal reads positive
    assert ledger.trial_balance() == 0


@pytest.mark.parametrize("entries", [
    [Entry("cash", debit=100), Entry("alice", credit=90)],          # does not balance
    [Entry("cash", debit=100)],                                      # single-sided
    [Entry("cash", debit=100, credit=100), Entry("alice", credit=0)],  # malformed lines
    [Entry("cash", debit=-5), Entry("alice", credit=-5)],            # negative amounts
])
def test_rejects_malformed_transactions(ledger, entries):
    seq = ledger.store.last_seq()
    with pytest.raises(UnbalancedTransaction):
        ledger.post(entries)
    assert ledger.store.last_seq() == seq  # nothing was written


def test_overdraft_is_rejected_atomically(ledger):
    ledger.transfer("cash", "alice", 1_000)
    seq, before = ledger.store.last_seq(), ledger.balances()
    with pytest.raises(InsufficientFunds):
        # Second leg is fine, first leg overdraws alice: the whole thing must fail.
        ledger.post([Entry("bob", debit=5_000), Entry("alice", credit=5_000)])
    assert ledger.store.last_seq() == seq and ledger.balances() == before


def test_unknown_account_and_duplicate_account(ledger):
    with pytest.raises(UnknownAccount):
        ledger.transfer("cash", "nobody", 1)
    with pytest.raises(AccountExists):
        ledger.open_account("cash", "Cash again")


def test_idempotency_key_posts_once(ledger):
    first = ledger.transfer("cash", "alice", 300, idempotency_key="req-1")
    second = ledger.transfer("cash", "alice", 300, idempotency_key="req-1")
    assert first == second and ledger.balance("alice") == 300


def test_events_cannot_be_edited_or_deleted(ledger):
    conn = ledger.store.conn
    with pytest.raises(sqlite3.DatabaseError, match="immutable"):
        conn.execute("UPDATE events SET data = '{}' WHERE seq = 1")
    with pytest.raises(sqlite3.DatabaseError, match="immutable"):
        conn.execute("DELETE FROM events")


def test_optimistic_concurrency_on_a_stream(ledger):
    store = ledger.store
    with store.transaction() as conn:
        store.append(conn, "doc-1", "Created", {}, expected_version=0)
    with pytest.raises(ConcurrencyError):
        with store.transaction() as conn:
            store.append(conn, "doc-1", "Edited", {}, expected_version=0)  # stale read
    assert [e.type for e in store.read("doc-1")] == ["Created"]


def test_rebuild_reproduces_the_projection_exactly(ledger):
    rng = random.Random(1)
    accounts = ["cash", "alice", "bob"]
    for n in range(200):
        src, dst = rng.sample(accounts, 2)
        try:
            ledger.transfer(src, dst, rng.randint(1, 4_000), idempotency_key=f"k{n}")
        except InsufficientFunds:
            pass
    original = ledger.post([Entry("cash", debit=1), Entry("equity", credit=1)], idempotency_key="fixed")
    before = ledger.balances()
    ledger.store.conn.execute("UPDATE accounts SET balance = 999999 WHERE id = 'bob'")  # corrupt the cache
    assert ledger.balances() != before
    ledger.rebuild()
    assert ledger.balances() == before and ledger.trial_balance() == 0
    # Idempotency survives a rebuild because keys live in the events.
    seq = ledger.store.last_seq()
    assert ledger.post([Entry("cash", debit=1), Entry("equity", credit=1)], idempotency_key="fixed") == original
    assert ledger.store.last_seq() == seq


def test_balance_as_of_and_statement(ledger):
    ledger.transfer("cash", "alice", 1_000, "first")
    checkpoint = ledger.store.last_seq()
    ledger.transfer("cash", "alice", 500, "second")
    ledger.transfer("alice", "bob", 200, "third")
    assert ledger.balance_as_of("alice", checkpoint) == 1_000
    assert ledger.balance("alice") == 1_300
    lines = ledger.statement("alice")
    assert [(l["description"], l["balance"]) for l in lines] == [("first", 1_000), ("second", 1_500), ("third", 1_300)]


def test_concurrent_transfers_never_overdraw_or_lose_money(tmp_path):
    path = str(tmp_path / "race.db")
    ledger = Ledger(EventStore(path))
    ledger.open_account("src", "Source", min_balance=0)
    ledger.open_account("dst", "Destination", min_balance=0)
    ledger.open_account("equity", "Equity", normal="credit")
    ledger.post([Entry("src", debit=1_000), Entry("equity", credit=1_000)])
    ok = []
    start = threading.Barrier(8)

    def worker():
        start.wait()
        wins = 0
        for _ in range(50):  # 8 x 50 x 10 = 4000 requested, only 1000 available
            try:
                ledger.transfer("src", "dst", 10)
                wins += 1
            except InsufficientFunds:
                pass
        ok.append(wins)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(ok) == 100
    assert ledger.balance("src") == 0 and ledger.balance("dst") == 1_000 and ledger.trial_balance() == 0
