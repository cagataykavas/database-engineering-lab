from datetime import UTC, datetime

import pytest

from dbbench.pagination import TransactionCursor


def test_cursor_round_trip_preserves_compound_seek_position() -> None:
    cursor = TransactionCursor(datetime(2026, 9, 11, 12, 30, tzinfo=UTC), 8841)
    decoded = TransactionCursor.decode(cursor.encode())
    assert decoded == cursor
    assert "+" not in cursor.encode()
    assert "/" not in cursor.encode()


@pytest.mark.parametrize("value", ["", "garbage", "e30", "eyJyb3dfaWQiOiAtMX0"])
def test_invalid_cursor_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="invalid transaction cursor"):
        TransactionCursor.decode(value)
