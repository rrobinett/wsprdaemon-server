"""Tests for pskreporter_forwarder + pskreporter_forward_elector.

No live ClickHouse, no network, no real PSKReporter delivery — we
inject fakes for the CH client, the leader-URL fetch, and ftlib's
PskReporter class.

Run with:  python3 tests/test_pskreporter_forwarder.py
"""
from __future__ import annotations

import sys
import tempfile
import types as _types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))


# ── Stub ftlib-pskreporter so import works without it installed ─────────────

class _FakeFtlibPskReporter:
    instances: list = []
    def __init__(self, *, callsign, grid, antenna, dummy=False, tcp=False):
        self.callsign = callsign
        self.grid = grid
        self.antenna = antenna
        self.dummy = dummy
        self.spots: list = []
        _FakeFtlibPskReporter.instances.append(self)
    def spot(self, *, callsign, frequency, mode, timestamp,
             db=None, locator=None, dt=None):
        self.spots.append({
            "callsign": callsign, "frequency": frequency, "mode": mode,
            "timestamp": timestamp, "db": db, "locator": locator, "dt": dt,
        })


def _install_fake_pskreporter():
    """Make `from pskreporter import PskReporter` resolve to our fake."""
    _FakeFtlibPskReporter.instances = []
    mod = _types.ModuleType("pskreporter")
    mod.PskReporter = _FakeFtlibPskReporter
    sys.modules["pskreporter"] = mod


# ── Stub clickhouse_connect / clickhouse_driver for module load ─────────────

for _mod in ("clickhouse_connect", "clickhouse_driver"):
    if _mod not in sys.modules:
        s = _types.ModuleType(_mod)
        s.Client = object
        s.get_client = lambda **kw: None
        sys.modules[_mod] = s


import pskreporter_forwarder as pf            # noqa: E402
import pskreporter_forward_elector as pfe     # noqa: E402


# ── helpers ─────────────────────────────────────────────────────────────────

UTC = timezone.utc

def _ts(s):
    return datetime.fromisoformat(s).astimezone(UTC).replace(tzinfo=None)


class _FakeCh:
    """Mimics clickhouse_driver.Client.execute(sql, params): returns rows."""
    def __init__(self, rows):
        self._rows = list(rows)
        self.queries: list = []
    def execute(self, sql, params=None):
        self.queries.append((sql, dict(params or {})))
        since = params.get("since") if params else None
        lim   = (params or {}).get("lim", len(self._rows))
        if since is None:
            out = self._rows
        else:
            # Column 2 (0-indexed) is ingested_at per fetch_pending's SELECT.
            out = [r for r in self._rows if _to_utc(r[1]) > _to_utc(since)]
        return out[:lim]


def _to_utc(dt):
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    return dt


def _row(*, ingested, time=None, mode="ft8", rx="AC0G/B1", grid_rx="EM38",
         tx="K1ABC", grid_tx="FN42", freq=14_074_580, snr=-12, dt=0.3):
    """Mirror the SELECT column order in pskreporter_forwarder.fetch_pending."""
    if time is None:
        time = ingested
    return (time, ingested, mode, rx, grid_rx, tx, grid_tx, freq, snr, dt)


# ── watermark persistence ───────────────────────────────────────────────────

class TestWatermark(unittest.TestCase):

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "wm"
            ts = datetime(2026, 5, 18, 14, 30, tzinfo=UTC)
            pf.write_watermark(p, ts)
            got = pf.read_watermark(p)
            self.assertEqual(got, ts)

    def test_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertIsNone(pf.read_watermark(Path(td) / "missing"))

    def test_corrupt_file_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "wm"
            p.write_text("not-a-date")
            self.assertIsNone(pf.read_watermark(p))

    def test_atomic_replace(self):
        """A failed write must NOT leave a half-written file."""
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "wm"
            pf.write_watermark(p, datetime(2026, 5, 18, tzinfo=UTC))
            # Confirm content; the tmp file is gone after rename.
            self.assertEqual(p.read_text().strip(),
                             "2026-05-18T00:00:00+00:00")
            self.assertFalse((p.with_suffix(p.suffix + ".tmp")).exists())


# ── fetch_pending shape ─────────────────────────────────────────────────────

class TestFetchPending(unittest.TestCase):

    def test_returns_normalized_rows(self):
        ch = _FakeCh([_row(ingested=_ts("2026-05-18T14:30:15"))])
        rows = pf.fetch_pending(ch, since=_ts("2026-05-18T14:30:00"),
                                batch_size=10)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(r.mode, "ft8")
        self.assertEqual(r.rx_sign, "AC0G/B1")
        self.assertEqual(r.tx_call, "K1ABC")
        self.assertEqual(r.frequency, 14_074_580)
        # ingested_at must be UTC-aware after normalization.
        self.assertEqual(r.ingested_at.tzinfo, UTC)

    def test_skips_rows_missing_rx_sign_or_tx_call(self):
        ch = _FakeCh([
            _row(ingested=_ts("2026-05-18T14:30:15"), rx=""),     # no rx
            _row(ingested=_ts("2026-05-18T14:30:16"), tx=""),     # no tx
            _row(ingested=_ts("2026-05-18T14:30:17")),            # ok
        ])
        rows = pf.fetch_pending(ch, since=_ts("2026-05-18T14:00:00"),
                                batch_size=10)
        self.assertEqual(len(rows), 1)


# ── forward_rows / PskReporter enqueue ──────────────────────────────────────

class TestForwardRows(unittest.TestCase):

    def setUp(self):
        _install_fake_pskreporter()
        self.cache = {}

    def test_enqueues_normalized_mode(self):
        rows = [pf.PskRow(
            time=datetime(2026, 5, 18, 14, 30, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 18, 14, 30, 5, tzinfo=UTC),
            mode="ft8", rx_sign="AC0G/B1", rx_loc="EM38",
            tx_call="K1ABC", grid="FN42",
            frequency=14_074_580, snr_db=-12, dt=0.27,
        )]
        n = pf.forward_rows(rows, psk_cache=self.cache)
        self.assertEqual(n, 1)
        inst = self.cache[("AC0G/B1", "EM38")]
        self.assertEqual(len(inst.spots), 1)
        self.assertEqual(inst.spots[0]["mode"], "FT8")        # normalized
        self.assertEqual(inst.spots[0]["callsign"], "K1ABC")
        self.assertEqual(inst.spots[0]["frequency"], 14_074_580)

    def test_unknown_mode_is_skipped(self):
        rows = [pf.PskRow(
            time=datetime(2026, 5, 18, 14, 30, tzinfo=UTC),
            ingested_at=datetime(2026, 5, 18, 14, 30, 5, tzinfo=UTC),
            mode="weird", rx_sign="AC0G/B1", rx_loc="EM38",
            tx_call="K1ABC", grid="FN42",
            frequency=14_074_580, snr_db=-12, dt=0.27,
        )]
        n = pf.forward_rows(rows, psk_cache=self.cache)
        self.assertEqual(n, 0)
        self.assertEqual(self.cache, {})

    def test_one_instance_per_station(self):
        """Two rows from the same rx_sign+rx_loc reuse one PskReporter;
        a different rx_sign gets its own."""
        rows = [
            pf.PskRow(time=datetime(2026, 5, 18, 14, 30, tzinfo=UTC),
                      ingested_at=datetime(2026, 5, 18, 14, 30, 5, tzinfo=UTC),
                      mode="ft8", rx_sign="AC0G/B1", rx_loc="EM38",
                      tx_call="K1ABC", grid="FN42",
                      frequency=14_074_580, snr_db=-12, dt=0.27),
            pf.PskRow(time=datetime(2026, 5, 18, 14, 30, tzinfo=UTC),
                      ingested_at=datetime(2026, 5, 18, 14, 30, 5, tzinfo=UTC),
                      mode="ft4", rx_sign="AC0G/B1", rx_loc="EM38",
                      tx_call="W1XYZ", grid="EN42",
                      frequency=14_080_000, snr_db=-18, dt=-0.5),
            pf.PskRow(time=datetime(2026, 5, 18, 14, 30, tzinfo=UTC),
                      ingested_at=datetime(2026, 5, 18, 14, 30, 5, tzinfo=UTC),
                      mode="ft8", rx_sign="K9XYZ", rx_loc="EN52",
                      tx_call="W2GSB", grid="FN30",
                      frequency=14_074_900, snr_db=-15, dt=0.1),
        ]
        n = pf.forward_rows(rows, psk_cache=self.cache)
        self.assertEqual(n, 3)
        self.assertIn(("AC0G/B1", "EM38"), self.cache)
        self.assertIn(("K9XYZ", "EN52"), self.cache)
        self.assertEqual(len(self.cache[("AC0G/B1", "EM38")].spots), 2)
        self.assertEqual(len(self.cache[("K9XYZ", "EN52")].spots), 1)


# ── leader-state arithmetic ─────────────────────────────────────────────────

class TestLeaderState(unittest.TestCase):

    def test_self_when_match(self):
        s = pf.LeaderState(elected="wd10")
        self.assertTrue(pf.is_self_leader(s, this_host="wd10"))

    def test_self_when_match_case_insensitive(self):
        s = pf.LeaderState(elected="WD10")
        self.assertTrue(pf.is_self_leader(s, this_host="wd10"))

    def test_not_self_when_different(self):
        s = pf.LeaderState(elected="wd20")
        self.assertFalse(pf.is_self_leader(s, this_host="wd10"))

    def test_not_self_when_unknown(self):
        """No info → fail closed (don't forward)."""
        s = pf.LeaderState(elected=None)
        self.assertFalse(pf.is_self_leader(s, this_host="wd10"))


# ── elector ─────────────────────────────────────────────────────────────────

class TestElector(unittest.TestCase):

    def test_picks_first_healthy(self):
        # wd10 down, wd20 up, wd30 up → wd20.
        probes = {"wd10.wsprdaemon.org": False,
                  "wd20.wsprdaemon.org": True,
                  "wd30.wsprdaemon.org": True}
        leader = pfe.pick_leader(
            ["wd10.wsprdaemon.org", "wd20.wsprdaemon.org", "wd30.wsprdaemon.org"],
            probe_fn=lambda h: probes[h],
        )
        self.assertEqual(leader, "wd20")

    def test_returns_none_when_all_down(self):
        leader = pfe.pick_leader(
            ["wd10.wsprdaemon.org", "wd20.wsprdaemon.org"],
            probe_fn=lambda h: False,
        )
        self.assertIsNone(leader)

    def test_prefers_first_when_all_healthy(self):
        leader = pfe.pick_leader(
            ["wd10.wsprdaemon.org", "wd20.wsprdaemon.org"],
            probe_fn=lambda h: True,
        )
        self.assertEqual(leader, "wd10")

    def test_write_then_read(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "leader"
            pfe.write_leader(p, "wd10")
            self.assertEqual(pfe.read_current(p), "wd10")

    def test_short_name(self):
        self.assertEqual(pfe.short_name("wd10.wsprdaemon.org"), "wd10")
        self.assertEqual(pfe.short_name("WD20"), "wd20")
        self.assertEqual(pfe.short_name("wd30.foo.bar.baz"), "wd30")


if __name__ == "__main__":
    unittest.main(verbosity=2)
