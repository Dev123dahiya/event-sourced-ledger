"""Demo: python -m ledger  — a small wallet system, then proof the log alone can rebuild it."""
from __future__ import annotations

import json
import os
import random
import tempfile

from . import Entry, EventStore, InsufficientFunds, Ledger


def main() -> None:
    path = os.path.join(tempfile.mkdtemp(), "ledger.db")
    ledger = Ledger(EventStore(path))
    ledger.open_account("bank", "Cash at bank", normal="debit")
    ledger.open_account("fees", "Fee revenue", normal="credit")
    users = [f"wallet-{i}" for i in range(1, 6)]
    for u in users:
        # Customer wallets are money we owe, so they are credit-normal and may not go below zero.
        ledger.open_account(u, u, normal="credit", min_balance=0)
        ledger.post([Entry("bank", debit=100_000), Entry(u, credit=100_000)], "deposit", idempotency_key=f"dep-{u}")

    rng = random.Random(42)
    posted = rejected = 0
    for n in range(500):
        src, dst = rng.sample(users, 2)
        amount = rng.randint(1_000, 60_000)
        fee = amount // 100
        try:
            # Three-legged transaction: the payer covers the amount plus a 1% fee.
            ledger.post([Entry(src, debit=amount + fee), Entry(dst, credit=amount), Entry("fees", credit=fee)],
                        "p2p payment", idempotency_key=f"pay-{n}")
            posted += 1
        except InsufficientFunds:
            rejected += 1

    before = ledger.balances()
    replayed = ledger.rebuild()
    print(json.dumps({
        "payments_posted": posted, "payments_rejected_insufficient_funds": rejected,
        "events_in_log": ledger.store.last_seq(), "events_replayed_on_rebuild": replayed,
        "projection_matches_after_rebuild": ledger.balances() == before,
        "trial_balance": ledger.trial_balance(), "balances": before,
    }, indent=2))


if __name__ == "__main__":
    main()
