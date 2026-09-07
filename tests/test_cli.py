"""Tests for the command line and the scheduler, against a local fixture server.

The exit codes are the part that matters most here. CI and the container healthcheck both read
them, so a `verify` that exits 0 on a store it could not confirm would turn a corrupted archive
into a green build.
"""
from __future__ import annotations

import http.server
import io
import json
import pathlib
import shutil
import tempfile
import threading
import unittest
from contextlib import redirect_stdout
from typing import Any

from hub.__main__ import EXIT_DATA, EXIT_OK, main
from hub.config import HubConfig
from hub.registry import Registry
from hub.scheduler import Scheduler

REPO = pathlib.Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "forexfactory_thisweek.json"


class _Feed:
    """A local stand-in for the calendar provider, so no test touches the network."""

    def __init__(self, body: bytes) -> None:
        self.calls = 0
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - the stdlib dictates this name
                outer.calls += 1
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: Any) -> None:
                return

        self._server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_port}/feed"

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


class CliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.root = self.tmp / "data"
        self.datasets = self.tmp / "datasets"
        self.datasets.mkdir()
        self.feed = _Feed(FIXTURE.read_bytes())
        original = (REPO / "datasets" / "cal.high_impact.yaml").read_text()
        rewritten = original.replace("https://nfs.faireconomy.media/ff_calendar_thisweek.json", self.feed.url)
        (self.datasets / "cal.high_impact.yaml").write_text(rewritten)

    def tearDown(self) -> None:
        self.feed.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def run_cli(self, *argv: str) -> tuple[int, str]:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = main(["--root", str(self.root), "--datasets", str(self.datasets), *argv])
        return code, buffer.getvalue()

    def test_ls_lists_the_dataset_with_its_schema_hash(self) -> None:
        code, out = self.run_cli("ls")
        self.assertEqual(code, EXIT_OK)
        self.assertIn("cal.high_impact", out)
        self.assertIn("sha256:", out)

    def test_validate_loads_every_schema(self) -> None:
        code, out = self.run_cli("validate")
        self.assertEqual(code, EXIT_OK)
        self.assertIn("1 dataset(s) valid", out)

    def test_collect_then_compile_then_verify_is_clean(self) -> None:
        self.assertEqual(self.run_cli("collect")[0], EXIT_OK)
        self.assertEqual(self.run_cli("compile")[0], EXIT_OK)
        code, out = self.run_cli("verify")
        self.assertEqual(code, EXIT_OK)
        self.assertIn("verified", out)

    def test_recollecting_unchanged_bytes_writes_nothing(self) -> None:
        self.run_cli("collect")
        _, out = self.run_cli("collect")
        self.assertIn("collected 0 record(s)", out)

    def test_verify_exits_three_on_a_tampered_snapshot(self) -> None:
        self.run_cli("collect")
        self.run_cli("compile")
        snapshot = next((self.root / "snapshot").rglob("*.qkh"))
        blob = bytearray(snapshot.read_bytes())
        blob[len(blob) // 2] ^= 0xFF
        snapshot.write_bytes(bytes(blob))
        self.assertEqual(self.run_cli("verify")[0], EXIT_DATA)

    def test_as_of_hides_facts_that_were_not_yet_knowable(self) -> None:
        self.run_cli("collect")
        journal = next((self.root / "journal").rglob("*.ndjson"))
        known_at = min(json.loads(line)["known_at"] for line in journal.read_text().splitlines() if line.strip())
        _, before = self.run_cli("as-of", "cal.high_impact", "USD", str(known_at - 1))
        _, after = self.run_cli("as-of", "cal.high_impact", "USD", str(known_at))
        self.assertEqual(before.strip(), "")
        self.assertNotEqual(after.strip(), "")

    def test_as_of_refuses_a_naive_instant(self) -> None:
        self.run_cli("collect")
        code, _ = self.run_cli("as-of", "cal.high_impact", "USD", "2026-09-10T08:15:00")
        self.assertEqual(code, EXIT_DATA)

    def test_health_reports_stale_before_any_run(self) -> None:
        code, out = self.run_cli("health")
        self.assertEqual(code, EXIT_DATA)
        self.assertIn("stale=true", out)

    def test_run_once_collects_compiles_and_beats_the_heartbeat(self) -> None:
        code, _ = self.run_cli("run", "--once")
        self.assertEqual(code, EXIT_OK)
        self.assertTrue((self.root / "heartbeat").exists())
        self.assertTrue(list((self.root / "snapshot").rglob("*.qkh")))
        self.assertEqual(self.run_cli("health")[0], EXIT_OK)


class SchedulerCadenceTest(unittest.TestCase):
    """The near-event tightening, which is what keeps `known_at` honest around a release."""

    def setUp(self) -> None:
        self.tmp = pathlib.Path(tempfile.mkdtemp())
        self.datasets = self.tmp / "datasets"
        self.datasets.mkdir()
        self.feed = _Feed(FIXTURE.read_bytes())
        original = (REPO / "datasets" / "cal.high_impact.yaml").read_text()
        (self.datasets / "cal.high_impact.yaml").write_text(
            original.replace("https://nfs.faireconomy.media/ff_calendar_thisweek.json", self.feed.url)
        )
        self.config = HubConfig.load(None, root_override=self.tmp / "data")
        self.registry = Registry.load(self.datasets)

    def tearDown(self) -> None:
        self.feed.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_interval_tightens_inside_an_event_window_and_relaxes_outside(self) -> None:
        event_ms = 1_789_043_400_000
        now = [event_ms / 1000]
        scheduler = Scheduler(self.config, self.registry, clock=lambda: now[0], sleep=lambda _s: None)
        state = scheduler._state["cal.high_impact"]
        state.events = (event_ms,)
        inside = scheduler._interval_for("cal.high_impact", state)
        now[0] = (event_ms + 86_400_000) / 1000
        outside = scheduler._interval_for("cal.high_impact", state)
        self.assertLess(inside, outside)
        self.assertEqual(inside, 30.0)

    def test_a_dataset_with_no_upcoming_event_uses_the_steady_cadence(self) -> None:
        scheduler = Scheduler(self.config, self.registry, clock=lambda: 1_789_000_000.0, sleep=lambda _s: None)
        state = scheduler._state["cal.high_impact"]
        state.events = ()
        self.assertEqual(scheduler._interval_for("cal.high_impact", state), 1800.0)


if __name__ == "__main__":
    unittest.main()
