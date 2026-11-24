"""The newest blessed sigmond release, for the board's VERSION column.

A bless publishes a GitHub Release on HamSCI/sigmond; a candidate has none, so
"releases/latest" names the newest blessed version.  latest() asks GitHub at most
once an hour (the board runs every 5 minutes; GitHub allows 60 unauthenticated
calls an hour) and keeps the answer in a small file.  When GitHub does not
answer, or answers with something that is not a release tag, the cached tag
stands; with no cache either, the answer is None and the board judges no station.

version_state() compares a station's aligned release (the drift report's
`release`, from the station's aligned.json) with that tag.
"""
from __future__ import annotations

import json
import re
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

API = "https://api.github.com/repos/HamSCI/sigmond/releases/latest"
CACHE_S = 3600
TIMEOUT_S = 10
_TAG = re.compile(r"^v(\d+)\.(\d+)(?:\.(\d+))?$")


def parse_tag(tag) -> tuple | None:
    """(major, minor, patch) for "v3.94" or "v3.94.1", else None."""
    m = _TAG.match(tag or "") if isinstance(tag, str) else None
    return None if m is None else (int(m[1]), int(m[2]), int(m[3] or 0))


def _read_cache(path: Path) -> dict:
    try:
        doc = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(doc, dict) or parse_tag(doc.get("tag")) is None:
        return {}
    return doc


def _ask_github(urlopen) -> str | None:
    req = urllib.request.Request(API, headers={"Accept": "application/vnd.github+json",
                                               "User-Agent": "wsprdaemon-fleetboard"})
    try:
        with urlopen(req, timeout=TIMEOUT_S) as r:
            tag = json.loads(r.read().decode("utf-8", "replace")).get("tag_name")
    except (OSError, ValueError, AttributeError):
        return None
    return tag if parse_tag(tag) is not None else None


def latest(cache_path, now: datetime, urlopen=urllib.request.urlopen,
           max_age_s: int = CACHE_S) -> str | None:
    """The newest blessed tag.  now: naive UTC."""
    path = Path(cache_path)
    cached = _read_cache(path)
    stamp = now.replace(tzinfo=timezone.utc).timestamp()
    if cached and 0 <= stamp - float(cached.get("asked_at", 0)) < max_age_s:
        return cached["tag"]
    tag = _ask_github(urlopen)
    if tag is None:
        return cached.get("tag")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"tag": tag, "asked_at": stamp}))
        tmp.replace(path)
    except OSError:
        pass  # an unwritable cache costs one GitHub call per run, nothing more
    return tag


def version_state(release, baseline, latest_tag) -> str:
    """current | behind | ahead | candidate | unknown | unjudged.  A candidate
    record at or below the latest blessed tag is judged by its number."""
    have = parse_tag(release)
    if have is None:
        return "unknown"
    want = parse_tag(latest_tag)
    # A canary aligned to a candidate keeps that record after the bless
    # (B4 and SIGMA read "v3.94 candidate" on 11-24, v3.94 blessed 11-21), so
    # only a candidate newer than the latest blessed tag reads as one.
    if baseline == "candidate" and (want is None or have > want):
        return "candidate"
    if want is None:
        return "unjudged"
    return "current" if have == want else ("behind" if have < want else "ahead")
