"""The hub's command line: run the daemon, or do one stage of its work by hand.

Every verb is usable on its own because that is what makes the pipeline debuggable -- `collect`
one dataset once, `compile` a window, `verify` the whole store, ask `as-of` what a consumer would
have seen at a given instant. The daemon is those same functions on a timer, not a second
implementation.

Exit codes are part of the contract: 0 success, 2 a usage mistake, 3 a data problem. `verify`
must never exit 0 on a store it could not confirm, because CI and the container healthcheck both
read that number and a false success is worse than a crash.
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
import time
from pathlib import Path
from typing import Any

from hub import __version__
from hub.collectors import Candidate
from hub.compiler import compile_all, verify
from hub.config import HubConfig
from hub.dedupe import RevisionIndex
from hub.journal import JournalWriter
from hub.logging import log
from hub.pipeline import ingest
from hub.quarantine import Quarantine
from hub.rawstore import RawStore
from hub.registry import Registry
from hub.scheduler import Scheduler
from hub.schema import DatasetSchema
from hubread.errors import HubError
from hubread.journal import read_range
from hubread.reader import Reader
from hubread.record import Availability

EXIT_OK = 0
EXIT_USAGE = 2
EXIT_DATA = 3


def _now_ms() -> int:
    return int(time.time() * 1000)


def _parse_instant(text: str) -> int:
    """Accept an ISO-8601 instant or a bare epoch-ms integer, and refuse a naive timestamp."""
    raw = text.strip()
    if raw.isdigit():
        return int(raw)
    parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise HubError(f"{text!r} has no UTC offset; an instant without a zone is ambiguous")
    return int(parsed.timestamp() * 1000)


def _history_for(root: Path, schema: DatasetSchema) -> tuple[RevisionIndex, dict[str, list[dict[str, Any]]]]:
    """Rebuild dedupe and derivation state from the journal so a restart continues cleanly.

    Without this a restarted hub would restart revisions at 1 and compute every z-score against
    an empty past, quietly producing different numbers for the same facts after a deploy.
    """
    index = RevisionIndex()
    history: dict[str, list[dict[str, Any]]] = {}
    try:
        records = read_range(root, schema.name, 0, 4_102_444_800_000)
    except HubError:
        return index, history
    index.rebuild_from(records)
    for record in records:
        history.setdefault(record.scope, []).append(dict(record.fields))
    return index, history


def collect_once(
    root: Path,
    registry: Registry,
    dataset: str,
    *,
    timeout: float,
    writer: JournalWriter | None = None,
) -> int:
    """Fetch one dataset once and journal whatever survives the pipeline. Returns records written."""
    source = registry.source(dataset)
    if source is None:
        raise HubError(f"dataset {dataset!r} has no collector; it is derived or internal")
    schema = registry.schema(dataset)
    blob = source.fetch(timeout)
    if blob is None:
        log(f"collect[{dataset}] no usable response; keeping the last known state")
        return 0
    raw_ref = RawStore(root).put(blob)
    candidates: list[Candidate] = source.parse(blob)
    index, history = _history_for(root, schema)
    result = ingest(
        candidates,
        schema,
        index,
        history,
        _now_ms(),
        source=getattr(source, "name", dataset),
        source_version=getattr(source, "url", ""),
        parser=source.parser,
        raw_ref=raw_ref,
        quarantine=Quarantine(root),
        max_observed_age_ms=source.max_observed_age_ms,
    )
    own_writer = writer is None
    handle = writer or JournalWriter(root)
    try:
        for record in result.records:
            handle.append(record)
    finally:
        if own_writer:
            handle.close()
    log(
        f"collect[{dataset}] written={result.written} duplicates={result.duplicates} "
        f"revisions={result.revisions} quarantined={result.quarantined}"
    )
    return result.written


def _cmd_ls(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    for name in registry.datasets():
        schema = registry.schema(name)
        kind = "collect" if registry.source(name) is not None else "internal"
        print(f"{name:28s} v{schema.version} {schema.scope_kind:10s} {kind:9s} {schema.hash()[:19]}")
    return EXIT_OK


def _cmd_validate(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    for name in registry.datasets():
        schema = registry.schema(name)
        print(f"ok {name} fields={len(schema.fields)} derived={len(schema.derived_fields())}")
    print(f"{len(registry.datasets())} dataset(s) valid")
    return EXIT_OK


def _cmd_collect(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    targets = [args.dataset] if args.dataset else registry.collecting()
    if not targets:
        log("no collecting datasets configured")
        return EXIT_OK
    written = 0
    writer = JournalWriter(config.root)
    try:
        for name in targets:
            written += collect_once(config.root, registry, name, timeout=config.timeout_seconds, writer=writer)
    finally:
        writer.close()
    print(f"collected {written} record(s) across {len(targets)} dataset(s)")
    return EXIT_OK


def _cmd_compile(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    manifest = compile_all(config.root, registry)
    total = sum(len(entry.windows) for entry in manifest.datasets.values())
    print(f"compiled {total} window(s) across {len(manifest.datasets)} dataset(s)")
    return EXIT_OK


def _cmd_verify(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    problems = verify(config.root, registry)
    for problem in problems:
        print(f"PROBLEM {problem}", file=sys.stderr)
    if problems:
        print(f"{len(problems)} problem(s)", file=sys.stderr)
        return EXIT_DATA
    print("store verified: every window recompiles to its recorded hash")
    return EXIT_OK


def _cmd_as_of(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    reader = Reader(config.root, config.policy)
    at = _parse_instant(args.at)
    records = reader.as_of(args.dataset, args.scope, at)
    for key in sorted(records):
        record = records[key]
        fields = " ".join(f"{k}={record.fields[k]}" for k in sorted(record.fields))
        print(f"{record.known_at} rev{record.revision} {record.availability} {key} | {fields}")
    print(f"{len(records)} fact(s) visible at {at}", file=sys.stderr)
    return EXIT_OK


def _cmd_health(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    health = Reader(config.root, config.policy).health()
    print(f"heartbeat_at={health.heartbeat_at}")
    print(f"age_ms={health.age_ms}")
    print(f"stale={str(health.stale).lower()}")
    return EXIT_DATA if health.stale else EXIT_OK


def _cmd_backfill(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    """Import records from an NDJSON file as `derived` availability unless they say otherwise.

    Backfill is deliberately a separate verb from `collect`: it is the only path that may write
    a `known_at` it did not observe, and stamping those records `derived` is what lets a strict
    consumer refuse them.
    """
    schema = registry.schema(args.dataset)
    path = Path(args.file)
    if not path.exists():
        raise HubError(f"backfill file not found: {path}")
    index, history = _history_for(config.root, schema)
    candidates: list[Candidate] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        import json

        row = json.loads(line)
        candidates.append(
            Candidate(
                scope=str(row["scope"]),
                key="",
                effective_at=int(row["effective_at"]),
                fields=dict(row.get("fields", {})),
                period_start=row.get("period_start"),
                period_end=row.get("period_end"),
                known_at=int(row["known_at"]),
                availability=Availability(row.get("availability", "derived")),
                title=str(row.get("title", "")),
            )
        )
    result = ingest(
        candidates,
        schema,
        index,
        history,
        _now_ms(),
        source=args.source,
        parser="backfill@1",
        quarantine=Quarantine(config.root),
        live=False,
    )
    writer = JournalWriter(config.root)
    try:
        for record in result.records:
            writer.append(record)
    finally:
        writer.close()
    print(f"backfilled {result.written} record(s), quarantined {result.quarantined}")
    return EXIT_OK


def _cmd_run(args: argparse.Namespace, config: HubConfig, registry: Registry) -> int:
    """The daemon: poll every collecting dataset on its cadence, compile, and beat the heartbeat."""
    scheduler = Scheduler(config, registry)
    return scheduler.run(once=args.once)


_COMMANDS = {
    "ls": _cmd_ls,
    "validate": _cmd_validate,
    "collect": _cmd_collect,
    "compile": _cmd_compile,
    "verify": _cmd_verify,
    "as-of": _cmd_as_of,
    "health": _cmd_health,
    "backfill": _cmd_backfill,
    "run": _cmd_run,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hub", description="qkt-data-hub: a point-in-time fact store")
    parser.add_argument("--version", action="version", version=f"qkt-data-hub {__version__}")
    parser.add_argument("--config", help="path to hub.yaml")
    parser.add_argument("--root", help="store root, overriding the config")
    parser.add_argument("--datasets", help="dataset directory, overriding the config")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ls", help="list datasets with their schema hashes")
    sub.add_parser("validate", help="load every dataset schema and report")
    collect = sub.add_parser("collect", help="fetch once and journal what survives")
    collect.add_argument("dataset", nargs="?", help="one dataset, or every collecting dataset")
    sub.add_parser("compile", help="compile journals into snapshots and rewrite the manifest")
    sub.add_parser("verify", help="recompile every window and compare against the manifest")
    as_of = sub.add_parser("as-of", help="what a consumer would have seen at an instant")
    as_of.add_argument("dataset")
    as_of.add_argument("scope")
    as_of.add_argument("at", help="ISO-8601 instant with an offset, or epoch milliseconds")
    sub.add_parser("health", help="report heartbeat freshness")
    backfill = sub.add_parser("backfill", help="import historical records from NDJSON")
    backfill.add_argument("dataset")
    backfill.add_argument("--file", required=True)
    backfill.add_argument("--source", default="backfill")
    run = sub.add_parser("run", help="run the collector daemon")
    run.add_argument("--once", action="store_true", help="one pass over every due source, then exit")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = HubConfig.load(args.config, root_override=Path(args.root) if args.root else None)
        datasets_dir = Path(args.datasets) if args.datasets else config.datasets_dir
        registry = Registry.load(datasets_dir)
        config.root.mkdir(parents=True, exist_ok=True)
        return _COMMANDS[args.command](args, config, registry)
    except HubError as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_DATA
    except (OSError, ValueError) as e:
        print(f"error: {e}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":
    sys.exit(main())
