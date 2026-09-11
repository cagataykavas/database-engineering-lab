class DatabaseLabError(Exception):
    """Base class for expected database engineering failures."""


class MigrationDriftError(DatabaseLabError):
    pass


class TransferConflict(DatabaseLabError):
    pass


class InsufficientFunds(DatabaseLabError):
    pass


class LeaseConflict(DatabaseLabError):
    pass
