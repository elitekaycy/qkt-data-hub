"""Runtime configuration, with unknown keys refused at load.

Every knob has the historical value as its default, so an omitted setting behaves exactly as
the system did before that setting existed. The strictness matters more than the defaults: a
typo like `refuse_derved` in a policy block would otherwise parse as absent and silently turn a
safety setting off, and the operator would have no way to see it.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from hub.simpleyaml import load_path
from hubread.errors import ConfigError
from hubread.policy import Policy

_TOP_KEYS = frozenset({"root", "datasets_dir", "policy", "poll", "server", "compile"})
_POLICY_KEYS = frozenset({"min_lag_ms", "refuse_derived", "stale_after_ms", "skew_tolerance_ms"})
_POLL_KEYS = frozenset({"timeout_seconds", "retry_seconds", "failures_before_alert", "heartbeat_seconds"})
_SERVER_KEYS = frozenset({"enabled", "host", "port", "token"})
_COMPILE_KEYS = frozenset({"every_seconds", "on_start"})


def _block(raw: Any, path: str, allowed: frozenset[str]) -> dict[str, Any]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{path} must be a mapping")
    if unknown := set(raw) - allowed:
        raise ConfigError(f"unknown {path} key(s) {sorted(unknown)}, allowed: {sorted(allowed)}")
    return raw


@dataclass(frozen=True, slots=True)
class HubConfig:
    """Where the store lives, how it polls, and what a reader is allowed to see."""

    root: Path
    datasets_dir: Path
    policy: Policy
    timeout_seconds: float = 30.0
    retry_seconds: float = 300.0
    failures_before_alert: int = 3
    heartbeat_seconds: float = 60.0
    server_enabled: bool = False
    server_host: str = "127.0.0.1"
    server_port: int = 8431
    server_token: str = ""
    compile_every_seconds: float = 3600.0
    compile_on_start: bool = True

    @classmethod
    def load(cls, path: Path | str | None, *, root_override: Path | None = None) -> HubConfig:
        raw: dict[str, Any] = {}
        if path is not None:
            parsed = load_path(path)
            raw = _block(parsed, "config", _TOP_KEYS)
        policy_raw = _block(raw.get("policy"), "policy", _POLICY_KEYS)
        poll = _block(raw.get("poll"), "poll", _POLL_KEYS)
        server = _block(raw.get("server"), "server", _SERVER_KEYS)
        compile_block = _block(raw.get("compile"), "compile", _COMPILE_KEYS)
        root = Path(root_override or raw.get("root") or "./data")
        return cls(
            root=root,
            datasets_dir=Path(raw.get("datasets_dir") or "./datasets"),
            policy=Policy(
                min_lag_ms=int(policy_raw.get("min_lag_ms", 0)),
                refuse_derived=bool(policy_raw.get("refuse_derived", False)),
                stale_after_ms=int(policy_raw.get("stale_after_ms", 900_000)),
                skew_tolerance_ms=int(policy_raw.get("skew_tolerance_ms", 5_000)),
            ),
            timeout_seconds=float(poll.get("timeout_seconds", 30.0)),
            retry_seconds=float(poll.get("retry_seconds", 300.0)),
            failures_before_alert=int(poll.get("failures_before_alert", 3)),
            heartbeat_seconds=float(poll.get("heartbeat_seconds", 60.0)),
            server_enabled=bool(server.get("enabled", False)),
            server_host=str(server.get("host", "127.0.0.1")),
            server_port=int(server.get("port", 8431)),
            server_token=str(server.get("token", "")),
            compile_every_seconds=float(compile_block.get("every_seconds", 3600.0)),
            compile_on_start=bool(compile_block.get("on_start", True)),
        )
