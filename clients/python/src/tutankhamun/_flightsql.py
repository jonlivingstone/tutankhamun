"""Minimal FlightSQL command encoding.

The server (arrow-flight 56) decodes a ``FlightDescriptor.cmd`` as a
``google.protobuf.Any`` wrapping a FlightSQL command message. We only ever send
``CommandStatementQuery`` / ``CommandStatementUpdate``, each of which is a single
``string query = 1`` field, so we hand-encode the protobuf rather than depend on
generated stubs. (pyarrow parses ``FlightInfo`` and tickets on the way back, so
nothing here needs decoding.)
"""

# arrow-flight builds the Any type_url as
# "type.googleapis.com/arrow.flight.protocol.sql.<MessageName>"
# (see arrow-flight-56.2.1/src/sql/mod.rs).
_TYPE_URL_BASE = "type.googleapis.com/arrow.flight.protocol.sql."


def _varint(n: int) -> bytes:
    """LEB128 unsigned varint."""
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def _len_delimited(field: int, payload: bytes) -> bytes:
    """A length-delimited (wire type 2) field. Field numbers are < 16 here, so
    the tag is a single byte."""
    tag = (field << 3) | 2
    return bytes((tag,)) + _varint(len(payload)) + payload


def _statement_message(sql: str) -> bytes:
    # CommandStatementQuery / CommandStatementUpdate: { string query = 1; }
    return _len_delimited(1, sql.encode("utf-8"))


def _pack_any(message_name: str, message: bytes) -> bytes:
    # google.protobuf.Any: { string type_url = 1; bytes value = 2; }
    type_url = (_TYPE_URL_BASE + message_name).encode("utf-8")
    return _len_delimited(1, type_url) + _len_delimited(2, message)


def statement_query_command(sql: str) -> bytes:
    """Encode a ``CommandStatementQuery`` as descriptor command bytes."""
    return _pack_any("CommandStatementQuery", _statement_message(sql))


def statement_update_command(sql: str) -> bytes:
    """Encode a ``CommandStatementUpdate`` as descriptor command bytes."""
    return _pack_any("CommandStatementUpdate", _statement_message(sql))
