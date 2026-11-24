"""The board's VERSION column: each station's aligned release against the
newest blessed one."""
import io
import json
from datetime import datetime, timedelta

from fleetboard import board, release
from fleetboard.sources import DriftRow, parse, queries

NOW = datetime(2026, 11, 24, 16, 0, 0)


# ── release.version_state ────────────────────────────────────────────────
def test_state_current_behind_ahead():
    assert release.version_state("v3.94", "blessed", "v3.94") == "current"
    assert release.version_state("v3.93", "blessed", "v3.94") == "behind"
    assert release.version_state("v3.95", "blessed", "v3.94") == "ahead"


def test_state_compares_numerically_not_as_text():
    assert release.version_state("v3.9", "blessed", "v3.10") == "behind"
    assert release.version_state("v3.100", "blessed", "v3.99") == "ahead"
    assert release.version_state("v3.94.1", "blessed", "v3.94") == "ahead"


def test_state_candidate_newer_than_blessed():
    assert release.version_state("v3.95", "candidate", "v3.94") == "candidate"
    assert release.version_state("v3.95", "candidate", None) == "candidate"


def test_state_candidate_of_a_blessed_release_is_judged_by_number():
    # B4 and SIGMA, 11-24: aligned to v3.94's candidate, v3.94 since blessed.
    assert release.version_state("v3.94", "candidate", "v3.94") == "current"
    assert release.version_state("v3.94", "candidate", "v3.95") == "behind"


def test_state_unknown_and_unjudged():
    assert release.version_state(None, "none", "v3.94") == "unknown"
    assert release.version_state("main", "blessed", "v3.94") == "unknown"
    assert release.version_state("v3.94", "blessed", None) == "unjudged"


# ── release.latest: GitHub, cached ───────────────────────────────────────
class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _gh(tag, calls):
    def urlopen(req, timeout=None):
        calls.append(req.full_url if hasattr(req, "full_url") else req)
        return _Resp(json.dumps({"tag_name": tag}).encode())
    return urlopen


def _down(calls):
    def urlopen(req, timeout=None):
        calls.append(1)
        raise OSError("network down")
    return urlopen


def test_latest_asks_github_and_caches(tmp_path):
    cache, calls = tmp_path / "latest.json", []
    assert release.latest(cache, NOW, urlopen=_gh("v3.94", calls)) == "v3.94"
    assert len(calls) == 1 and "HamSCI/sigmond/releases/latest" in calls[0]
    # Within the hour the cache answers; GitHub is not asked again.
    assert release.latest(cache, NOW + timedelta(minutes=30), urlopen=_gh("v3.95", calls)) == "v3.94"
    assert len(calls) == 1
    # After the hour GitHub is asked again.
    assert release.latest(cache, NOW + timedelta(minutes=61), urlopen=_gh("v3.95", calls)) == "v3.95"
    assert len(calls) == 2


def test_latest_keeps_the_cached_tag_when_github_fails(tmp_path):
    cache, calls = tmp_path / "latest.json", []
    release.latest(cache, NOW, urlopen=_gh("v3.94", calls))
    assert release.latest(cache, NOW + timedelta(hours=2), urlopen=_down(calls)) == "v3.94"


def test_latest_is_none_with_no_cache_and_no_github(tmp_path):
    assert release.latest(tmp_path / "latest.json", NOW, urlopen=_down([])) is None


def test_latest_refuses_a_tag_that_is_not_a_release(tmp_path):
    calls = []
    assert release.latest(tmp_path / "a.json", NOW, urlopen=_gh("nightly", calls)) is None
    cache = tmp_path / "b.json"
    release.latest(cache, NOW, urlopen=_gh("v3.94", calls))
    # A bad answer later keeps the good cached tag.
    assert release.latest(cache, NOW + timedelta(hours=2), urlopen=_gh("nightly", calls)) == "v3.94"


def test_latest_survives_an_unreadable_cache(tmp_path):
    cache = tmp_path / "latest.json"
    cache.write_text("not json")
    assert release.latest(cache, NOW, urlopen=_gh("v3.94", [])) == "v3.94"


# ── sources: the drift query carries release and baseline ────────────────
def test_drift_query_selects_release_and_baseline():
    sql = queries(NOW)["drift_latest"]
    assert "release" in sql and "baseline" in sql


def test_parse_reads_release_and_baseline():
    res = {"drift_latest": [("A", NOW, NOW, 0, 0, "[]", "[]", "v3.94", "blessed")]}
    row = parse(res, NOW).drift["A"]
    assert (row.release, row.baseline) == ("v3.94", "blessed")


def test_parse_reads_an_empty_release_as_none():
    res = {"drift_latest": [("A", NOW, NOW, 0, 0, "[]", "[]", "", "none")]}
    assert parse(res, NOW).drift["A"].release is None


# ── board: the version cell ──────────────────────────────────────────────
class _SA:
    def __init__(self, drift):
        self.drift = drift


def test_version_json_judges_the_station_against_latest():
    sa = _SA(DriftRow(NOW, NOW, release="v3.93", baseline="blessed"))
    v = json.loads(board._version_json(sa, NOW, "v3.94"))
    assert v == {"release": "v3.93", "baseline": "blessed", "latest": "v3.94",
                 "state": "behind", "stale": False}


def test_version_json_marks_an_old_report_stale():
    sa = _SA(DriftRow(NOW, NOW - timedelta(days=3), release="v3.94", baseline="blessed"))
    assert json.loads(board._version_json(sa, NOW, "v3.94"))["stale"] is True


def test_version_json_is_empty_without_a_report():
    assert board._version_json(_SA(None), NOW, "v3.94") == ""


def test_schema_has_a_version_column():
    assert ("version", "String") in board.SCHEMAS["fleet_board"]
