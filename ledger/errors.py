class LedgerError(Exception):
    """Base class for every rejection the ledger can raise."""


class AccountExists(LedgerError):
    pass


class UnknownAccount(LedgerError):
    pass


class UnbalancedTransaction(LedgerError):
    pass


class InsufficientFunds(LedgerError):
    pass


class ConcurrencyError(LedgerError):
    """The stream moved on since the caller last read it."""
