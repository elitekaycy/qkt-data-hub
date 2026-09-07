# Repository Instructions

This file is the root instruction file for automated coding work in this repository. Keep it
synchronized with `CLAUDE.md`; this file is the concise always-read version.

## Working Style

- Be direct and pragmatic. Call out wrong assumptions or risky requests with reasons.
- Make the smallest reasonable change that actually solves the problem.
- Prefer simple, readable, maintainable code over clever code.
- Match surrounding style exactly.
- Fix bugs found while working when they are in scope.
- Do not rewrite or throw away existing implementations without explicit approval.

## Data rules (this repo produces the history other systems are graded against)

A backtest cites a snapshot from this store. If a fact here is wrong, or was visible earlier than
it should have been, every result computed from it is wrong in a way nobody can see. These rules
are what stop that.

- **`known_at` is recorded, never guessed.** Live collection stamps it from the ingest clock and
  sets `availability: observed`. Inference from a schedule or a fixed lag happens only in an
  explicit backfill and is stamped `derived`, which a consumer may refuse outright. Never upgrade
  a `derived` record to `observed`.
- **Three timestamps, never two.** `period` is what a fact describes, `effective_at` is when it
  happens or applies, `known_at` is when we could know it. Collapsing any two of them is how
  look-ahead gets in. The visibility rule is `known_at <= T` and nothing else.
- **Append only.** Nothing under `raw/`, `journal/` or `snapshot/` is ever rewritten. A correction
  is a new revision of the same `key`. The one exception is repairing a torn trailing line at
  writer open, and even that quarantines the discarded bytes rather than dropping them.
- **Numbers are decimal strings.** `hubread.record.decimal_str` is the only producer of canonical
  decimal text. A `float` anywhere in a value or comparison path is a bug, including in a range
  check — `float(100000000000000003) == float(100000000000000000)`.
- **Timestamps are UTC epoch milliseconds, `int`.** A naive source timestamp is rejected, never
  assumed to be UTC. A sibling project shipped a bug where a New York timestamp was read as UTC
  and every event window moved four hours.
- **Unknown is not zero.** A missing value is `None` and stays `None` through every derivation.
  The moment "no data" silently becomes `0`, every downstream comparison lies.
- **Fail closed.** An unknown key in a schema or config, an unparseable record, an unexpected
  content type from a source: raise or quarantine, never default. A typo must never silently
  disable a check.
- **Determinism is testable, so test it.** Compiling the same journal in any line order must
  produce byte-identical snapshot bytes. Re-running a parse over the raw archive must reproduce
  the same payloads. Those tests exist; do not weaken them to make a change pass.
- **One writer per hub root**, enforced by an exclusive lock. Two writers would interleave `seq`
  and corrupt the ordering everything else depends on.
- **Provenance travels with the fact.** Every record carries its source, source version, parser
  version and raw blob hash, in the journal AND in the snapshot. A snapshot that cannot say where
  its facts came from is not auditable.

## Architecture

- `hubread/` **owns the format** and is standalone: stdlib only, and it must never import `hub`.
  A consumer that cannot afford a dependency — a kill-switch daemon, a trading engine's data path
  — vendors this alone and can decode every artifact. Verify with a grep before committing.
- `hub/` is the **writer**: collectors, pipeline, journal writer, compiler, server, CLI. It imports
  `hubread` freely. `hub/record.py` and `hub/errors.py` are thin re-export shims.
- Collectors do I/O and nothing else. Parse, clean, normalise, validate, deduplicate and derive are
  pure functions of their inputs — that purity is what makes the pipeline replayable.
- The hub holds **no broker credentials** and has no path to a trading gateway. It informs; it
  never decides or trades.

## Working in this codebase

- Zero runtime dependencies, Python 3.12 stdlib only, including the YAML loader
  (`hub/simpleyaml.py`, a deliberately restricted subset). `ruff` and `mypy` are dev-only and
  never ship in the image.
- Before committing: `python3 -m ruff check hub hubread tests`, `python3 -m mypy`, and
  `python3 -m unittest discover -s tests -t .`. CI runs the same three plus a Docker smoke test
  that runs the real pipeline against a local fixture server.
- A new dataset is a YAML file in `datasets/`, not code. Reach for a code collector only when a
  declarative mapping genuinely cannot express the source.
- Never add a network call to a test. Fixtures under `tests/fixtures/` are real captured bytes;
  add to them rather than reaching for the internet.
- Docstrings explain **why** a thing exists or what invariant it protects, not what the next line
  does. No emoji anywhere. No AI references in code, comments, commits or docs.
- Commits: Conventional Commits, subject only, imperative, lowercase after the colon, at most 70
  characters, no body, no trailer, no co-author line.
