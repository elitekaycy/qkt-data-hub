"""Fetching over HTTP, politely, and refusing to believe a response that is the wrong shape.

The failure mode this guards against is specific and was observed in a sibling project: a feed
returned an HTML error page with a 200 status, the JSON parser produced nothing, and the system
carried on looking healthy while its event windows quietly emptied. A wrong content type is
therefore a hard failure here, not a parse that happens to yield zero rows.

Conditional requests are used where the server supports them so an unchanged document costs one
round trip and no bytes, and so the raw archive is not asked to store the same page twice.
"""
from __future__ import annotations

import time
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from hub.logging import log
from hub.rawstore import RawBlob

DEFAULT_USER_AGENT = "qkt-data-hub/0.1 (+https://github.com/elitekaycy/qkt-data-hub)"


class HttpSource:
    """Fetches one URL and hands the bytes on untouched.

    `expect_content_type` is a substring match against the response's Content-Type. It is
    required rather than optional: a source that does not state what it expects cannot detect
    the day its provider starts answering with something else.
    """

    def __init__(
        self,
        name: str,
        url: str,
        *,
        expect_content_type: str,
        headers: dict[str, str] | None = None,
        opener: Callable[..., Any] = urllib.request.urlopen,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._name = name
        self._url = url
        self._expect = expect_content_type.lower()
        self._headers = {"User-Agent": DEFAULT_USER_AGENT, **(headers or {})}
        self._open = opener
        self._clock = clock
        self._etag: str | None = None
        self._last_modified: str | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def url(self) -> str:
        return self._url

    def fetch(self, timeout_seconds: float) -> RawBlob | None:
        """Return the response bytes, or `None` on any failure or an unchanged document.

        Never raises: the scheduler treats `None` as "retry sooner, keep what you had", which is
        how one dead provider avoids taking the others down with it.
        """
        headers = dict(self._headers)
        if self._etag:
            headers["If-None-Match"] = self._etag
        if self._last_modified:
            headers["If-Modified-Since"] = self._last_modified
        request = urllib.request.Request(self._url, headers=headers)
        try:
            with self._open(request, timeout=timeout_seconds) as response:
                body = response.read()
                info = response.headers
                content_type = str(info.get("Content-Type", "")).lower()
                etag = info.get("ETag")
                last_modified = info.get("Last-Modified")
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return None
            log(f"collect[{self._name}] http {e.code} from {self._url}")
            return None
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            log(f"collect[{self._name}] fetch error: {e}")
            return None
        if self._expect not in content_type:
            log(f"collect[{self._name}] refused: expected content-type {self._expect!r}, got {content_type!r}")
            return None
        if not body:
            log(f"collect[{self._name}] refused: empty body")
            return None
        self._etag = etag
        self._last_modified = last_modified
        return RawBlob(
            body=body,
            content_type=content_type,
            fetched_at=int(self._clock() * 1000),
            request={"url": self._url, "method": "GET"},
        )
