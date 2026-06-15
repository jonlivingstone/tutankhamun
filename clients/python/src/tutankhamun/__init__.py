"""Tutankhamun — native Python client for the Tutankhamun analytics engine.

    import tutankhamun as tk

    conn = tk.connect("grpc://localhost:50051")
    with conn.session(dataset="nyc_taxi") as s:
        s.define("extras", "total_amount - fare_amount")
        table = (
            s.filter("tpep_pickup_datetime >= TIMESTAMP '2024-01-01'")
             .group_by("date_trunc('month', tpep_pickup_datetime)")
             .select("date_trunc('month', tpep_pickup_datetime) AS month, "
                     "count(*) AS trips, sum(extras) AS extras_cents")
             .fetch()
        )
        df = table.to_pandas()        # native; or tk.to_polars(table)
"""

from .client import Connection
from .errors import QueryError, SessionLost, TutankhamunError

__version__ = "0.1.0"

__all__ = [
    "connect",
    "Connection",
    "TutankhamunError",
    "QueryError",
    "SessionLost",
    "to_polars",
    "__version__",
]


def connect(uri: str, **client_kwargs) -> Connection:
    """Connect to a Tutankhamun daemon.

    ``uri`` is a Flight URI, e.g. ``"grpc://localhost:50051"`` (or
    ``"grpc+tls://host:port"``). Extra keyword arguments pass through to
    ``pyarrow.flight.FlightClient``.
    """
    return Connection(uri, **client_kwargs)


def to_polars(table):
    """Convert a result ``pyarrow.Table`` to a polars DataFrame (zero-copy).

    Requires the optional ``polars`` dependency.
    """
    try:
        import polars as pl
    except ImportError as e:  # pragma: no cover - exercised only without polars
        raise TutankhamunError(
            "to_polars requires polars; install tutankhamun[polars] or `pip install polars`"
        ) from e
    return pl.from_arrow(table)
