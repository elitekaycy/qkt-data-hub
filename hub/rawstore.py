"""The raw archive: every byte a source returned, kept forever, addressed by content.

A parser is a hypothesis about someone else's format, and hypotheses turn out wrong. When one
does, the only way to correct history honestly is to re-run the corrected parser over exactly
the bytes the old one saw and emit new revisions. That is only possible if those bytes were kept.

Blobs are content-addressed, so re-fetching an unchanged page costs nothing and two collectors
that pull the same document store it once. Every record the pipeline writes carries the hash of
the blob it came from, which is what makes a fact traceable back to its evidence.
"""
from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from hubread.errors import StoreError


@dataclass(frozen=True, slots=True)
class RawBlob:
    """One response, exactly as it arrived, plus what we asked to get it."""

    body: bytes
    content_type: str
    fetched_at: int
    request: dict[str, str] = field(default_factory=dict)

    def sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.body).hexdigest()


class RawStore:
    """Content-addressed blob storage under `<root>/raw/<aa>/<sha256>`.

    Writes are atomic and idempotent: the same bytes always land at the same path, so a retry
    or a duplicate fetch cannot corrupt or duplicate an entry.
    """

    def __init__(self, root: Path) -> None:
        self._root = Path(root) / "raw"

    def _path(self, digest: str) -> Path:
        hexpart = digest.split(":", 1)[1]
        return self._root / hexpart[:2] / hexpart

    def put(self, blob: RawBlob) -> str:
        """Store `blob` and return its content address. Storing twice is a no-op."""
        digest = blob.sha256()
        target = self._path(digest)
        if target.exists():
            return digest
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp")
        tmp.write_bytes(blob.body)
        os.replace(tmp, target)
        meta = {
            "sha256": digest,
            "content_type": blob.content_type,
            "fetched_at": blob.fetched_at,
            "bytes": len(blob.body),
            "request": blob.request,
        }
        meta_path = target.with_suffix(".meta.json")
        meta_tmp = target.with_suffix(".meta.tmp")
        meta_tmp.write_text(json.dumps(meta, sort_keys=True, separators=(",", ":")), encoding="utf-8")
        os.replace(meta_tmp, meta_path)
        return digest

    def get(self, digest: str) -> RawBlob:
        """Read a blob back by content address, for a re-parse over archived evidence."""
        target = self._path(digest)
        if not target.exists():
            raise StoreError(f"raw blob {digest} is not in the archive")
        meta_path = target.with_suffix(".meta.json")
        meta: dict[str, Any] = {}
        if meta_path.exists():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        return RawBlob(
            body=target.read_bytes(),
            content_type=str(meta.get("content_type", "")),
            fetched_at=int(meta.get("fetched_at", 0)),
            request={str(k): str(v) for k, v in dict(meta.get("request", {})).items()},
        )

    def has(self, digest: str) -> bool:
        return self._path(digest).exists()
