"""End-to-end tests against a live daemon.

Skipped unless a daemon is reachable: set ``TUT_TEST_ADDR`` to an already-running
daemon's Flight URI, or have a built ``t9n`` binary plus seed ``nyc_taxi`` storage
under ``.cache/storage`` (the fixture then spawns one on a private port).
"""

import os
import pathlib
import subprocess
import time
import urllib.request

import pytest

import tutankhamun as tk

REPO_ROOT = pathlib.Path(__file__).resolve().parents[3]
_GRPC_ADDR = "127.0.0.1:50071"
_OPS_ADDR = "127.0.0.1:18091"


def _find_binary():
    for variant in ("release", "debug"):
        candidate = REPO_ROOT / "target" / variant / "t9n"
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def _wait_ready(ops_addr, timeout=20.0):
    deadline = time.time() + timeout
    url = f"http://{ops_addr}/readyz"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    return True
        except Exception:  # noqa: BLE001 - daemon not up yet
            time.sleep(0.2)
    return False


@pytest.fixture(scope="session")
def daemon_uri():
    env_addr = os.environ.get("TUT_TEST_ADDR")
    if env_addr:
        yield env_addr
        return

    binary = _find_binary()
    storage = REPO_ROOT / ".cache" / "storage"
    if binary is None or not (storage / "nyc_taxi").is_dir():
        pytest.skip(
            "no t9n binary or seed nyc_taxi storage; build t9n and ingest nyc_taxi, "
            "or set TUT_TEST_ADDR to a running daemon"
        )

    cache = REPO_ROOT / ".cache" / "cache-pytest"
    proc = subprocess.Popen(
        [
            str(binary),
            "serve",
            "--grpc-addr",
            _GRPC_ADDR,
            "--ops-addr",
            _OPS_ADDR,
            "--storage-url",
            f"file://{storage}",
            "--cache-dir",
            str(cache),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        if not _wait_ready(_OPS_ADDR):
            pytest.skip("daemon did not become ready")
        yield f"grpc://{_GRPC_ADDR}"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()


def test_count(daemon_uri):
    with tk.connect(daemon_uri).session(dataset="nyc_taxi") as s:
        table = s.select("count(*) AS n").fetch()
        assert table.num_rows == 1
        assert table.column("n")[0].as_py() > 0


def test_grouped_aggregate_pushdown(daemon_uri):
    with tk.connect(daemon_uri).session(dataset="nyc_taxi") as s:
        table = (
            s.group_by("(fare_amount > 10)")
            .select("(fare_amount > 10) AS hi, count(*) AS n")
            .fetch()
        )
        assert table.num_rows >= 1
        assert set(table.column_names) >= {"hi", "n"}


def test_define_macro_end_to_end(daemon_uri):
    with tk.connect(daemon_uri).session(dataset="nyc_taxi") as s:
        baseline = s.select("sum(fare_amount) AS f").fetch().column("f")[0].as_py()
        s.define("double_fare", "fare_amount * 2")
        doubled = s.select("sum(double_fare) AS f").fetch().column("f")[0].as_py()
        assert doubled == baseline * 2


def test_session_token_issued(daemon_uri):
    with tk.connect(daemon_uri).session(dataset="nyc_taxi") as s:
        assert s.token  # an opaque UUID string
