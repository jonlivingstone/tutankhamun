"""Unit tests for SQL composition, define() macros, and command encoding.

These use a stub connection, so they need no daemon (only pyarrow importable,
the package's hard dependency).
"""

import pytest

from tutankhamun import _flightsql
from tutankhamun.errors import SessionLost
from tutankhamun.session import Session


class StubConnection:
    """Records executed SQL; never touches the network."""

    def __init__(self):
        self.queries = []
        self.opens = 0

    def _handshake(self):
        self.opens += 1
        return [(b"authorization", b"Bearer tok%d" % self.opens)]

    def _run_query(self, sql, headers):
        self.queries.append(sql)
        return "RESULT"

    def _close_session(self, headers):
        pass


def session(from_clause="nyc_taxi"):
    return Session(StubConnection(), from_clause)


# --- composition ----------------------------------------------------------


def test_select_group_filter_compose():
    q = session().filter("payment_type = 1").group_by("passenger_count").select(
        "count(*) AS n"
    )
    assert q.to_sql() == (
        "SELECT count(*) AS n FROM nyc_taxi "
        "WHERE (payment_type = 1) GROUP BY passenger_count"
    )


def test_bare_session_is_select_star():
    assert Session(StubConnection(), "nyc_taxi")._compose((), (), None) == (
        "SELECT * FROM nyc_taxi"
    )


def test_multiple_filters_anded():
    q = session().filter("a = 1").filter("b = 2")
    assert q.to_sql() == "SELECT * FROM nyc_taxi WHERE (a = 1) AND (b = 2)"


def test_multiple_group_keys():
    q = session().group_by("a", "b").select("count(*)")
    assert q.to_sql() == "SELECT count(*) FROM nyc_taxi GROUP BY a, b"


def test_query_is_immutable():
    base = session().filter("a = 1")
    extended = base.filter("b = 2")
    assert base.to_sql() == "SELECT * FROM nyc_taxi WHERE (a = 1)"
    assert extended.to_sql() == "SELECT * FROM nyc_taxi WHERE (a = 1) AND (b = 2)"


def test_compose_over_subquery_base():
    q = Session(StubConnection(), "(SELECT * FROM t WHERE x > 1) AS _t").select("count(*)")
    assert q.to_sql() == "SELECT count(*) FROM (SELECT * FROM t WHERE x > 1) AS _t"


# --- define() macros ------------------------------------------------------


def test_define_adds_computed_column():
    s = session()
    s.define("net", "fare_amount - tip_amount")
    q = s.group_by("passenger_count").select("sum(net) AS net")
    assert q.to_sql() == (
        "SELECT sum(net) AS net "
        "FROM (SELECT *, (fare_amount - tip_amount) AS net FROM nyc_taxi) AS _t "
        "GROUP BY passenger_count"
    )


def test_multiple_defines_are_independent_columns():
    s = session()
    s.define("a", "x + 1")
    s.define("b", "y * 2")
    assert s.select("count(*)").to_sql() == (
        "SELECT count(*) FROM (SELECT *, (x + 1) AS a, (y * 2) AS b FROM nyc_taxi) AS _t"
    )


def test_define_rejects_bad_name():
    with pytest.raises(ValueError):
        session().define("1bad", "x")


# --- execution + replay ---------------------------------------------------


def test_fetch_executes_composed_sql():
    conn = StubConnection()
    Session(conn, "nyc_taxi").filter("a = 1").select("count(*)").fetch()
    assert conn.queries == ["SELECT count(*) FROM nyc_taxi WHERE (a = 1)"]


def test_fetch_replays_once_on_session_lost():
    class FlakyConnection(StubConnection):
        def __init__(self):
            super().__init__()
            self._fail_next = True

        def _run_query(self, sql, headers):
            if self._fail_next:
                self._fail_next = False
                raise SessionLost("session not found or expired")
            return super()._run_query(sql, headers)

    conn = FlakyConnection()
    s = Session(conn, "nyc_taxi")  # opens once
    assert s.select("count(*)").fetch() == "RESULT"
    assert conn.opens == 2  # reopened after the loss
    assert conn.queries == ["SELECT count(*) FROM nyc_taxi"]


# --- command encoding -----------------------------------------------------


def test_varint():
    assert _flightsql._varint(0) == b"\x00"
    assert _flightsql._varint(127) == b"\x7f"
    assert _flightsql._varint(128) == b"\x80\x01"
    assert _flightsql._varint(300) == b"\xac\x02"


def test_statement_query_command_bytes():
    cmd = _flightsql.statement_query_command("SELECT 1")
    inner = b"\x0a\x08SELECT 1"  # field 1 (query), len 8
    type_url = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    expected = (
        b"\x0a" + bytes((len(type_url),)) + type_url  # Any.type_url (field 1)
        + b"\x12" + bytes((len(inner),)) + inner       # Any.value (field 2)
    )
    assert cmd == expected
