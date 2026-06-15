"""Connection — the FlightSQL transport over ``pyarrow.flight``.

Owns the ``FlightClient`` and the low-level RPCs the :class:`Session` drives:
the handshake (mints a bearer token), statement execution, and the custom
``CloseSession`` action. End users go through :func:`tutankhamun.connect`.
"""

import pyarrow.flight as flight

from ._flightsql import statement_query_command
from .errors import QueryError, SessionLost, TutankhamunError
from .session import Session

_CLOSE_SESSION_ACTION = "CloseSession"


class Connection:
    """A connection to a Tutankhamun daemon's FlightSQL endpoint."""

    def __init__(self, uri: str, **client_kwargs):
        self._uri = uri
        self._client = flight.FlightClient(uri, **client_kwargs)

    def session(self, dataset: str | None = None, *, time_range=None) -> Session:
        """Open a session over a dataset.

        ``time_range`` is not supported yet (it needs a known time column);
        express a time filter via :meth:`session_from_sql` meanwhile.
        """
        if time_range is not None:
            raise NotImplementedError(
                "time_range is not supported yet; express the time filter via "
                "session_from_sql()"
            )
        if not dataset:
            raise ValueError("session(dataset=...) requires a dataset name")
        return Session(self, dataset)

    def session_from_sql(self, sql: str) -> Session:
        """Open a session whose base relation is an arbitrary SQL query."""
        if not sql or not sql.strip():
            raise ValueError("session_from_sql() requires a non-empty SQL string")
        return Session(self, f"({sql}) AS _base")

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False

    # --- wire helpers used by Session -----------------------------------

    def _handshake(self):
        """Perform the handshake; return the bearer call headers.

        The server's handshake mints a token and returns it as the standard
        ``authorization: Bearer <token>`` response header, which
        ``authenticate_basic_token`` surfaces (the server ignores the sent
        credentials).
        """
        try:
            header, value = self._client.authenticate_basic_token(b"", b"")
        except Exception as e:  # noqa: BLE001 - surface any handshake failure uniformly
            raise TutankhamunError(f"handshake failed: {e}") from e
        return [(header, value)]

    def _run_query(self, sql: str, headers):
        opts = flight.FlightCallOptions(headers=headers)
        descriptor = flight.FlightDescriptor.for_command(statement_query_command(sql))
        try:
            info = self._client.get_flight_info(descriptor, opts)
            reader = self._client.do_get(info.endpoints[0].ticket, opts)
            return reader.read_all()
        except flight.FlightError as e:
            _raise_mapped(e)

    def _close_session(self, headers) -> None:
        opts = flight.FlightCallOptions(headers=headers)
        action = flight.Action(_CLOSE_SESSION_ACTION, b"")
        try:
            for _ in self._client.do_action(action, opts):
                pass
        except flight.FlightError:
            # Best-effort: an unknown/expired token is a no-op, and the idle
            # reaper reclaims anything we fail to close.
            pass


def _raise_mapped(error: "flight.FlightError"):
    # pyarrow.flight doesn't surface the gRPC status code, so we match the
    # server's stable SessionLost message ("session not found or expired; ...").
    # Requiring "session" avoids misclassifying e.g. a "table ... not found".
    msg = str(error)
    lowered = msg.lower()
    if "session" in lowered and ("not found" in lowered or "expired" in lowered):
        raise SessionLost(msg) from error
    raise QueryError(msg) from error
