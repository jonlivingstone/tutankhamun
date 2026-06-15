"""Exception types raised by the Tutankhamun client."""


class TutankhamunError(Exception):
    """Base class for all client errors."""


class QueryError(TutankhamunError):
    """The server rejected or failed a query (parse error, budget exceeded, etc.)."""


class SessionLost(TutankhamunError):
    """The session token is unknown or expired server-side (reaped or closed).

    The client reopens and replays transparently on `fetch`; this surfaces only
    if a reopen also fails.
    """
