"""The station-level `forward_to_wsprnet` switch, and the identity it carries.

rob's spec: "that proxy upload column was to be sent by the client ... this
would be a single setting that would apply to all spots."  So the station
declares ONE flag in `routing.json`, and the server turns it into the
`proxy_upload` column the wsprnet forwarder selects on.  No schema migration:
the column has existed since the WsprDaemon client first wrote field 33.

Three properties carry the weight here:

  * Silence is NOT "off".  The legacy client decides per spot in field 33.
    If a missing routing.json key read as False, upgrading a server would
    switch those stations' proxy uploads off from a file they never shipped
    — and the symptom would be spots quietly absent from wsprnet.org.
  * Present means it GOVERNS, every spot of that receiver, because the
    setting is station-level by design.
  * An identity with no callsign is dropped whole, never half-applied.

No ClickHouse: these are the parsing helpers against synthetic trees.

Run with:  python3 -m pytest tests/test_wsprnet_routing.py -v
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import types as _types  # noqa: E402
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

import wsprdaemon_server as ws  # noqa: E402

SITE = "AC0G=B4_EM38ww"
RECV = "KA9Q_0"
KEY = f"{SITE}/{RECV}"

# A real 34-field enhanced line, field 33 (proxy_upload) last.
_FIELDS = [
    "261027", "1430", "0", "-12", "0.3", "14.097123", "W2NAF", "FN21", "37",
    "0", "1", "0", "0", "1500", "0", "1", "0", "2", "-120.0", "-125.0",
    "20", "EM38ww", "AC0G", "1234", "45.0", "38.9", "-94.6", "70.0",
    "41.0", "-75.0", "40.0", "-85.0", "0",
]


def _tree(tmp: Path, *, proxy_field: str = "0", n: int = 2) -> Path:
    d = tmp / "wspr" / "spots" / SITE / RECV / "20"
    d.mkdir(parents=True)
    line = " ".join(_FIELDS + [proxy_field])
    (d / "261027_1430_spots.txt").write_text("\n".join([line] * n) + "\n")
    return tmp


def _spots(routing, *, proxy_field="0", n=2):
    with tempfile.TemporaryDirectory() as td:
        return ws.process_spot_files(_tree(Path(td), proxy_field=proxy_field,
                                           n=n), "4.0", routing)


# ---------- the three-valued flag ----------

def test_a_missing_key_leaves_field_33_ALONE():
    """⛔ Silence is not 'off'.  A station on the legacy client decides per
    spot in field 33; if absence read as False, a server upgrade would
    switch it off from a file it never sent."""
    assert [s["proxy_upload"] for s in _spots({}, proxy_field="1")] == [1, 1]
    assert [s["proxy_upload"] for s in _spots({}, proxy_field="0")] == [0, 0]
    # ...and routing.json carrying only the psk key is still silence here.
    only_psk = {"default": {"forward_to_pskreporter": True}}
    assert [s["proxy_upload"]
            for s in _spots(only_psk, proxy_field="1")] == [1, 1]


def test_an_explicit_true_governs_EVERY_spot_overriding_field_33():
    r = {"default": {"forward_to_wsprnet": True}}
    assert [s["proxy_upload"] for s in _spots(r, proxy_field="0")] == [1, 1]


def test_an_explicit_false_governs_EVERY_spot_overriding_field_33():
    """The off switch has to actually switch off, or a station that stops
    proxying keeps being proxied and double-posts to wsprnet."""
    r = {"default": {"forward_to_wsprnet": False}}
    assert [s["proxy_upload"] for s in _spots(r, proxy_field="1")] == [0, 0]


def test_a_per_receiver_key_beats_the_default():
    r = {"default": {"forward_to_wsprnet": False},
         KEY: {"forward_to_wsprnet": True}}
    assert [s["proxy_upload"] for s in _spots(r)] == [1, 1]


def test_another_receivers_key_does_not_apply_to_this_one():
    r = {"default": {"forward_to_wsprnet": False},
         f"{SITE}/OTHER_RX": {"forward_to_wsprnet": True}}
    assert [s["proxy_upload"] for s in _spots(r)] == [0, 0]


def test_the_flag_helper_is_three_valued():
    assert ws._wsprnet_flag({}, KEY) is None
    assert ws._wsprnet_flag({"default": {"forward_to_wsprnet": False}},
                            KEY) is False
    assert ws._wsprnet_flag({KEY: {"forward_to_wsprnet": True}}, KEY) is True


# ---------- the declared identity ----------

def test_a_declared_identity_is_stamped_and_collapses_to_one_row():
    """One row per receiver, not per spot: a tar with 400 spots declares one
    identity, and 400 inserts would make the rarest fact the busiest write."""
    r = {"default": {"forward_to_wsprnet": True,
                     "wsprnet_reporter": {"call": "AC0G", "grid": "EM38"}}}
    spots = _spots(r, n=50)
    rows = ws.wsprnet_reporter_rows(spots)
    assert len(rows) == 1
    assert rows[0]["key"] == "AC0G/KA9Q_0"     # <rx_sign>/<rx_id>
    assert rows[0]["call"] == "AC0G"
    assert rows[0]["grid"] == "EM38"


def test_the_identity_key_is_what_the_forwarder_LOOKS_UP():
    """These two spellings must not drift: the server writes the key and the
    forwarder reads it, in different files, with nothing joining them."""
    sys.path.insert(0, str(HERE.parent))
    import wsprnet_forwarder as wf
    r = {"default": {"wsprnet_reporter": {"call": "AC0G/M", "grid": "EM38"}}}
    rows = ws.wsprnet_reporter_rows(_spots(r))
    row = wf.SpotRow(time=None, ingested_at=None, rx_sign="AC0G",
                     rx_loc="EM38ww", rx_id="KA9Q_0", tx_sign="W2NAF",
                     tx_loc="FN21", frequency_mhz=14.097, snr=-12, drift=0,
                     power=37, sync_quality=0.8, dt=0.3, code=2)
    assert wf.reporter_key(row) == rows[0]["key"]
    overrides = {rows[0]["key"]: {"call": rows[0]["call"],
                                  "grid": rows[0]["grid"]}}
    assert wf.reporter_for(row, overrides) == ("AC0G/M", "EM38")


def test_an_identity_with_no_call_is_dropped_WHOLE_not_half_applied():
    """⛔ A grid alone cannot name a reporter.  Taking the stated grid while
    inventing the call from rx_sign would post real spots from a location
    the operator never claimed for that identity."""
    r = {"default": {"wsprnet_reporter": {"grid": "EM38"}}}
    assert ws._wsprnet_reporter(r, KEY) is None
    assert ws.wsprnet_reporter_rows(_spots(r)) == []


def test_no_declaration_means_no_identity_row_at_all():
    """The forwarder's default — the row's own rx_sign — is correct for every
    single-receiver site, so a site that declares nothing must write nothing
    rather than a row restating rx_sign."""
    assert ws.wsprnet_reporter_rows(_spots({})) == []
    r = {"default": {"forward_to_wsprnet": True}}
    assert ws.wsprnet_reporter_rows(_spots(r)) == []


def test_a_grid_may_be_omitted_when_the_call_is_given():
    r = {KEY: {"wsprnet_reporter": {"call": "AC0G"}}}
    assert ws._wsprnet_reporter(r, KEY) == ("AC0G", "")


def test_a_malformed_identity_declaration_does_not_raise():
    for bad in ("AC0G", ["AC0G"], 7, None):
        assert ws._wsprnet_reporter({KEY: {"wsprnet_reporter": bad}},
                                    KEY) is None


# ---------- the stamp must not reach ClickHouse ----------

def test_the_stamp_keys_never_become_columns():
    """`_wsprnet_call` is scratch state on the spot dict.  If it reached the
    insert, the whole batch would fail — every station's spots in that
    flush, not just this one's."""
    r = {"default": {"wsprnet_reporter": {"call": "AC0G", "grid": "EM38"}}}
    spot = _spots(r)[0]
    assert "_wsprnet_call" in spot
    ch = ws.convert_spot_to_clickhouse(spot)
    assert not [k for k in ch if k.startswith("_")]


# ---------- the row and the table must agree ----------

def test_every_identity_row_key_is_a_column_in_the_schema():
    """⛔ A row key with no column fails the INSERT — and since the DDL
    lives in two files (schema/wsprnet_reporter.sql and the inline copy a
    fresh wd bootstraps from), the two can drift apart silently."""
    import re
    r = {"default": {"wsprnet_reporter": {"call": "AC0G", "grid": "EM38"}}}
    row_keys = set(ws.wsprnet_reporter_rows(_spots(r))[0])
    sql_file = (HERE.parent / "schema" / "wsprnet_reporter.sql").read_text()
    src = (HERE.parent / "wsprdaemon_server.py").read_text()
    inline = src.split("CREATE TABLE IF NOT EXISTS {db}.{WSPRNET_REPORTER_TABLE}")[1]
    inline = inline.split('""")')[0]
    for text, where in ((sql_file, "schema file"), (inline, "inline DDL")):
        cols = set(re.findall(r"`(\w+)`\s+(?:String|DateTime)", text))
        assert row_keys <= cols, f"{where} is missing {row_keys - cols}"
        assert cols == row_keys, f"{where} has extra columns {cols - row_keys}"


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))
