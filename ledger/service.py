from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Iterable

from .errors import AccountExists, InsufficientFunds, UnbalancedTransaction, UnknownAccount
from .store import Event, EventStore

PROJECTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id          TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    normal      TEXT NOT NULL CHECK (normal IN ('debit', 'credit')),
    min_balance INTEGER,              -- in normal-balance terms; NULL = unbounded
    balance     INTEGER NOT NULL DEFAULT 0   -- debits minus credits, in minor units
);
CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, txn_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS projection_state (id INTEGER PRIMARY KEY CHECK (id = 1), last_seq INTEGER NOT NULL);
INSERT OR IGNORE INTO projection_state (id, last_seq) VALUES (1, 0);
"""


@dataclass(frozen=True)
class Entry:
    """One line of a transaction. Amounts are positive integers in minor units (paise, cents)."""

    account: str
    debit: int = 0
    credit: int = 0

    @property
    def signed(self) -> int:
        return self.debit - self.credit


def _normal_balance(raw: int, normal: str) -> int:
    """Balance from the account's own point of view (positive is the usual direction)."""
    return raw if normal == "debit" else -raw


class Ledger:
    """Commands validate against current state, then append events.

    The ``accounts`` table is a projection: a cache of what the events say. It is
    updated in the same database transaction as the event, so it is never stale,
    and ``rebuild()`` can recreate it from the log alone.
    """

    def __init__(self, store: EventStore) -> None:
        self.store = store
        store.conn.executescript(PROJECTION_SCHEMA)

    # ---- commands --------------------------------------------------------
    def open_account(self, account_id: str, name: str, normal: str = "debit", min_balance: int | None = None) -> Event:
        if normal not in ("debit", "credit"):
            raise ValueError("normal must be 'debit' or 'credit'")
        with self.store.transaction() as conn:
            if conn.execute("SELECT 1 FROM accounts WHERE id = ?", (account_id,)).fetchone():
                raise AccountExists(account_id)
            event = self.store.append(
                conn, f"account-{account_id}", "AccountOpened",
                {"id": account_id, "name": name, "normal": normal, "min_balance": min_balance}, expected_version=0,
            )
            self._apply(conn, event)
            return event

    def post(self, entries: Iterable[Entry], description: str = "", idempotency_key: str | None = None) -> str:
        """Post a balanced transaction atomically. Returns the transaction id.

        Either every entry is recorded or none is. A repeated idempotency key
        returns the original transaction id without posting again.
        """
        entries = list(entries)
        self._validate_shape(entries)
        with self.store.transaction() as conn:
            if idempotency_key is not None:
                seen = conn.execute("SELECT txn_id FROM idempotency WHERE key = ?", (idempotency_key,)).fetchone()
                if seen:
                    return seen["txn_id"]
            # Net effect per account, then check every limit before writing anything.
            net: dict[str, int] = {}
            for e in entries:
                net[e.account] = net.get(e.account, 0) + e.signed
            for account_id, delta in net.items():
                row = conn.execute("SELECT normal, min_balance, balance FROM accounts WHERE id = ?", (account_id,)).fetchone()
                if row is None:
                    raise UnknownAccount(account_id)
                if row["min_balance"] is not None:
                    after = _normal_balance(row["balance"] + delta, row["normal"])
                    if after < row["min_balance"]:
                        raise InsufficientFunds(f"{account_id}: balance would be {after}, minimum is {row['min_balance']}")
            txn_id = uuid.uuid4().hex
            event = self.store.append(
                conn, f"txn-{txn_id}", "TransactionPosted",
                {"id": txn_id, "description": description, "idempotency_key": idempotency_key,
                 "entries": [{"account": e.account, "debit": e.debit, "credit": e.credit} for e in entries]},
                expected_version=0,
            )
            self._apply(conn, event)
            return txn_id

    def transfer(self, source: str, destination: str, amount: int, description: str = "",
                 idempotency_key: str | None = None) -> str:
        """Move value between two debit-normal accounts: credit the source, debit the destination."""
        return self.post([Entry(destination, debit=amount), Entry(source, credit=amount)], description, idempotency_key)

    @staticmethod
    def _validate_shape(entries: list[Entry]) -> None:
        if len(entries) < 2:
            raise UnbalancedTransaction("a transaction needs at least two entries")
        for e in entries:
            if e.debit < 0 or e.credit < 0 or (e.debit == 0) == (e.credit == 0):
                raise UnbalancedTransaction(f"{e.account}: each entry needs exactly one positive side")
            if not isinstance(e.debit, int) or not isinstance(e.credit, int):
                raise UnbalancedTransaction("amounts must be integers in minor units")
        total = sum(e.signed for e in entries)
        if total != 0:
            raise UnbalancedTransaction(f"debits and credits differ by {total}")

    # ---- projection ------------------------------------------------------
    @staticmethod
    def _apply(conn, event: Event) -> None:
        """Fold one event into the projection. The only place projections change."""
        if event.type == "AccountOpened":
            d = event.data
            conn.execute("INSERT INTO accounts (id, name, normal, min_balance) VALUES (?, ?, ?, ?)",
                         (d["id"], d["name"], d["normal"], d["min_balance"]))
        elif event.type == "TransactionPosted":
            for e in event.data["entries"]:
                conn.execute("UPDATE accounts SET balance = balance + ? WHERE id = ?", (e["debit"] - e["credit"], e["account"]))
            if event.data.get("idempotency_key"):
                conn.execute("INSERT INTO idempotency (key, txn_id) VALUES (?, ?)", (event.data["idempotency_key"], event.data["id"]))
        conn.execute("UPDATE projection_state SET last_seq = ? WHERE id = 1", (event.seq,))

    def rebuild(self) -> int:
        """Throw the projection away and recreate it by replaying every event."""
        with self.store.transaction() as conn:
            conn.execute("DELETE FROM accounts")
            conn.execute("DELETE FROM idempotency")
            conn.execute("UPDATE projection_state SET last_seq = 0 WHERE id = 1")
            count = 0
            for event in list(self.store.read()):
                self._apply(conn, event)
                count += 1
            return count

    # ---- queries ---------------------------------------------------------
    def balance(self, account_id: str) -> int:
        """Current balance from the account's own point of view."""
        row = self.store.conn.execute("SELECT normal, balance FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if row is None:
            raise UnknownAccount(account_id)
        return _normal_balance(row["balance"], row["normal"])

    def balances(self) -> dict[str, int]:
        rows = self.store.conn.execute("SELECT id, normal, balance FROM accounts ORDER BY id").fetchall()
        return {r["id"]: _normal_balance(r["balance"], r["normal"]) for r in rows}

    def balance_as_of(self, account_id: str, seq: int) -> int:
        """Time travel: the balance as it stood after event number ``seq``, by replay."""
        normal, raw = None, 0
        for event in self.store.read(up_to_seq=seq):
            if event.type == "AccountOpened" and event.data["id"] == account_id:
                normal = event.data["normal"]
            elif event.type == "TransactionPosted":
                raw += sum(e["debit"] - e["credit"] for e in event.data["entries"] if e["account"] == account_id)
        if normal is None:
            raise UnknownAccount(account_id)
        return _normal_balance(raw, normal)

    def statement(self, account_id: str) -> list[dict]:
        """Every entry that touched an account, with a running balance."""
        normal = self.store.conn.execute("SELECT normal FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if normal is None:
            raise UnknownAccount(account_id)
        lines, raw = [], 0
        for event in self.store.read():
            if event.type != "TransactionPosted":
                continue
            for e in event.data["entries"]:
                if e["account"] == account_id:
                    raw += e["debit"] - e["credit"]
                    lines.append({"seq": event.seq, "txn": event.data["id"], "description": event.data["description"],
                                  "debit": e["debit"], "credit": e["credit"], "balance": _normal_balance(raw, normal["normal"])})
        return lines

    def trial_balance(self) -> int:
        """Sum of all raw balances. Double entry guarantees this is always 0."""
        return self.store.conn.execute("SELECT COALESCE(SUM(balance), 0) FROM accounts").fetchone()[0]
