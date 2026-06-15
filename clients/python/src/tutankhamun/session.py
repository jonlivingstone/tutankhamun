"""Session and the lazy fluent query composer.

A :class:`Session` holds an open server session (a bearer token) plus the
client-side state the design keeps in the library: the base relation and any
``define``-d named metrics. :class:`Query` is immutable — each ``filter`` /
``group_by`` / ``select`` returns a new query — and ``fetch`` composes the SQL
and runs it in the session.
"""

import re

from .errors import SessionLost

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


class Query:
    """An immutable, lazily-composed query bound to a :class:`Session`."""

    def __init__(self, session, *, filters=(), group_keys=(), projection=None):
        self._session = session
        self._filters = tuple(filters)
        self._group_keys = tuple(group_keys)
        self._projection = projection

    def filter(self, predicate: str) -> "Query":
        """Add a SQL predicate (ANDed with any existing filters)."""
        return Query(
            self._session,
            filters=self._filters + (predicate,),
            group_keys=self._group_keys,
            projection=self._projection,
        )

    def group_by(self, *keys: str) -> "Query":
        """Group by one or more SQL expressions."""
        return Query(
            self._session,
            filters=self._filters,
            group_keys=self._group_keys + keys,
            projection=self._projection,
        )

    def select(self, projection: str) -> "Query":
        """Set the projection (a SQL select list, e.g. ``"count(*), sum(x)"``)."""
        return Query(
            self._session,
            filters=self._filters,
            group_keys=self._group_keys,
            projection=projection,
        )

    def to_sql(self) -> str:
        """The composed SQL this query would execute (defines expanded)."""
        return self._session._compose(
            self._filters, self._group_keys, self._projection
        )

    def fetch(self):
        """Execute and return the result as a ``pyarrow.Table``."""
        return self._session._execute(self.to_sql())


class Session:
    """An open server session and the client-side workflow state."""

    def __init__(self, connection, base: str):
        self._connection = connection
        self._base = base
        self._defines: dict[str, str] = {}
        self._headers = None
        self._closed = False
        self._open()

    def _open(self) -> None:
        self._headers = self._connection._handshake()

    @property
    def token(self) -> str | None:
        """The opaque server-issued session token (parsed from the bearer header)."""
        if self._headers is None:
            return None
        return self._headers[0][1].split(b" ", 1)[-1].decode()

    # --- workflow state -------------------------------------------------

    def define(self, name: str, expr: str) -> "Session":
        """Define a named metric usable as a column in later fragments.

        The metric is folded into the base relation as a computed column
        (``SELECT *, (expr) AS name FROM <base>``) rather than rewritten into
        each fragment, so user aliases and identifiers are never disturbed and it
        survives session reaping with no server state. Defines are independent:
        one metric cannot reference another.
        """
        if not _IDENT.fullmatch(name):
            raise ValueError(f"invalid metric name: {name!r}")
        self._defines[name] = expr
        return self

    # --- fluent entry points (delegate to a fresh Query) ----------------

    def filter(self, predicate: str) -> Query:
        return Query(self).filter(predicate)

    def group_by(self, *keys: str) -> Query:
        return Query(self).group_by(*keys)

    def select(self, projection: str) -> Query:
        return Query(self).select(projection)

    def fetch(self):
        """Fetch the whole base relation (``SELECT *``) as a ``pyarrow.Table``."""
        return Query(self).fetch()

    def sql(self, raw_sql: str):
        """Run an arbitrary SQL statement in this session; returns a ``pyarrow.Table``."""
        return self._execute(raw_sql)

    # --- composition + execution ----------------------------------------

    def _from_clause(self) -> str:
        if not self._defines:
            return self._base
        cols = ", ".join(f"({expr}) AS {name}" for name, expr in self._defines.items())
        return f"(SELECT *, {cols} FROM {self._base}) AS _t"

    def _compose(self, filters, group_keys, projection) -> str:
        proj = projection if projection else "*"
        sql = f"SELECT {proj} FROM {self._from_clause()}"
        if filters:
            conj = " AND ".join(f"({f})" for f in filters)
            sql += f" WHERE {conj}"
        if group_keys:
            sql += " GROUP BY " + ", ".join(group_keys)
        return sql

    def _execute(self, sql: str):
        if self._closed:
            raise SessionLost("session is closed")
        try:
            return self._connection._run_query(sql, self._headers)
        except SessionLost:
            # The token was reaped or invalidated; reopen once and retry. Defines
            # are client-side, so there is no server state to replay.
            self._open()
            return self._connection._run_query(sql, self._headers)

    def close(self) -> None:
        """Close the session server-side, freeing its state immediately."""
        if self._closed or self._headers is None:
            return
        self._connection._close_session(self._headers)
        self._closed = True

    def __enter__(self) -> "Session":
        return self

    def __exit__(self, *exc) -> bool:
        self.close()
        return False
