"""Tests for the read-only web view: it answers from the store's files and never writes.

The UI is what an operator trusts to say "the hub is healthy", so it must report what the store
actually holds (records, windows, the newest visible fact) and must leave the store untouched.
"""
import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from hub.__main__ import main
from hub.compiler import compile_all
from hub.journal import JournalWriter
from hub.registry import Registry
from hub.ui import make_handler, state
from hubread.policy import Policy
from hubread.record import Availability, Record

DATASET = """
dataset: cal.us_test
version: 1
title: test calendar
scope_kind: currency
key: [effective_at, scope]
fields:
  at: { type: timestamp, null_policy: forbid }
"""


def _record(seq_at: int, at: int) -> Record:
    return Record.create(
        dataset="cal.us_test", scope="USD", key=f"{at}|USD", revision=1, known_at=seq_at, effective_at=at,
        period_start=at, period_end=None, availability=Availability.DERIVED, source="test", source_version="",
        parser="test@1", raw_ref=None, fields={"at": at},
    )


class UiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        base = Path(self.tmp.name)
        (base / "datasets").mkdir()
        (base / "datasets" / "cal.us_test.yaml").write_text(DATASET)
        self.root = base / "store"
        self.registry = Registry.load(base / "datasets")
        writer = JournalWriter(self.root)
        for known, at in ((1_704_067_200_000, 1_706_727_600_000), (1_706_727_660_000, 1_710_957_600_000)):
            writer.append(_record(known, at))
        writer.close()
        compile_all(self.root, self.registry)

    def tearDown(self):
        self.tmp.cleanup()

    def _files(self):
        return {p: p.stat().st_mtime_ns for p in self.root.rglob("*") if p.is_file()}

    def test_state_reports_what_the_store_holds(self):
        doc = state(self.root, self.registry, Policy())
        (d,) = doc["datasets"]
        self.assertEqual(d["journal"]["records"], 2)
        self.assertEqual(d["journal"]["scopes"], ["USD"])
        self.assertEqual(d["manifest"]["last_seq"], d["journal"]["last_seq"])
        self.assertTrue(doc["compile"]["windows"] >= 1)

    def test_http_api_answers_and_leaves_the_store_untouched(self):
        before = self._files()
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.root, self.registry, Policy()))
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{httpd.server_address[1]}"
        try:
            q = f"{base}/api/as-of?dataset=cal.us_test&scope=USD&at="
            facts = json.load(urllib.request.urlopen(q + "2024-02-01T00:00:00Z"))
            self.assertEqual([f["fields"]["at"] for f in facts], [1_706_727_600_000, 1_710_957_600_000])
            early = json.load(urllib.request.urlopen(q + "1704067199999"))
            self.assertEqual(early, [])
            self.assertIn(b"qkt-data-hub", urllib.request.urlopen(f"{base}/").read())
            with self.assertRaises(urllib.error.HTTPError) as e:
                urllib.request.urlopen(q + "2024-02-01T00:00:00")
            self.assertEqual(e.exception.code, 400)
        finally:
            httpd.shutdown()
            httpd.server_close()
        self.assertEqual(self._files(), before)

    def test_run_serves_the_view_from_the_daemons_own_process(self):
        # One container runs the whole hub: `run --ui-port` serves the view while the collector
        # runs, and closes it when the collector stops.
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        url = f"http://127.0.0.1:{port}/api/state"
        seen = {}

        def collector(_self, once=False):
            seen["datasets"] = [d["name"] for d in json.load(urllib.request.urlopen(url))["datasets"]]
            return 0

        datasets = str(Path(self.tmp.name) / "datasets")
        with mock.patch("hub.__main__.Scheduler.run", collector):
            code = main(["--root", str(self.root), "--datasets", datasets, "run", "--once", "--ui-port", str(port)])
        self.assertEqual(code, 0)
        self.assertEqual(seen["datasets"], ["cal.us_test"])
        with self.assertRaises(urllib.error.URLError):
            urllib.request.urlopen(url, timeout=2)

    def test_run_without_a_ui_port_serves_nothing(self):
        datasets = str(Path(self.tmp.name) / "datasets")
        with mock.patch("hub.__main__.Scheduler.run", return_value=0), mock.patch("hub.ui.bind") as bind:
            self.assertEqual(main(["--root", str(self.root), "--datasets", datasets, "run", "--once"]), 0)
        bind.assert_not_called()


if __name__ == "__main__":
    unittest.main()
