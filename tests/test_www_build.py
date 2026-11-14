"""www build: page.html plus logic.js become one HTML file for a Google Sites embed block.
No browser: the tests read the built file as text."""
import os
import re
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BUILD = os.path.join(REPO, "sigmondb", "www", "build_page.py")

CDN = [
    "https://cdnjs.cloudflare.com/ajax/libs/d3/7.9.0/d3.min.js",
    "https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js",
    "https://cdn.jsdelivr.net/npm/us-atlas@3.0.1/states-10m.json",
]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    out = tmp_path_factory.mktemp("www") / "transmitter-map.html"
    run = subprocess.run([sys.executable, BUILD, str(out)], capture_output=True, text=True, check=True)
    html = out.read_text()
    return html, run.stdout


def test_build_inlines_logic(built):
    html, _ = built
    assert "function parseHash" in html
    assert "<!-- LOGIC -->" not in html


def test_build_prints_size(built):
    html, stdout = built
    assert str(len(html.encode())) in stdout


def test_build_pins_cdn_urls(built):
    html, _ = built
    for url in CDN:
        assert url in html
    srcs = re.findall(r'<script[^>]*\ssrc="([^"]+)"', html)
    assert sorted(srcs) == sorted(CDN[:2])


def test_build_reads_only_public(built):
    html, _ = built
    assert "sigmondb_public" in html
    for bad in ("wd30", "sigmondb.arrival", "FROM sigmondb."):
        assert bad not in html


def test_build_has_both_views(built):
    html, _ = built
    assert 'data-view="timing"' in html
    assert 'data-view="ionosphere"' in html
    timing = re.search(r'<[^>]*data-view="timing"[^>]*>', html).group(0)
    assert 'aria-pressed="true"' in timing
    iono = re.search(r'<[^>]*data-view="ionosphere"[^>]*>', html).group(0)
    assert 'aria-pressed="false"' in iono


def test_build_standalone_url_defaults_empty(built):
    html, _ = built
    assert 'const STANDALONE_URL = "";' in html


def test_build_embeds_standalone_url(tmp_path):
    out = tmp_path / "t.html"
    url = "https://example.org/sigmond/map.html"
    subprocess.run([sys.executable, BUILD, "--standalone-url", url, str(out)], check=True, capture_output=True)
    assert f'const STANDALONE_URL = "{url}";' in out.read_text()


def test_build_refuses_unsafe_standalone_url(tmp_path):
    run = subprocess.run([sys.executable, BUILD, "--standalone-url", "https://x/</script>", str(tmp_path / "t.html")],
                         capture_output=True, text=True)
    assert run.returncode != 0


def test_refresh_always_applies_and_never_spreads_rows(built):
    """page.html has no browser test: read the built text.  refresh() must catch a prepare()
    failure into load.error and still reach apply(); builtAt must not spread rows."""
    html, _ = built
    assert "} catch (e) {" in html.split("function refresh()")[1].split("function prepare")[0]
    assert "...TABLES_READ.flatMap" not in html
    assert "maxBuiltAt(d, TABLES_READ)" in html


# A claude.ai artifact sandbox blocks fetches to outside servers, so a review copy carries a
# snapshot of the public tables; the default build stays live.
def _snapshot(tmp_path):
    snap = {"taken_utc": "2026-11-14T03:10Z",
            "tables": {"heard": {"meta": [{"name": "n_24h", "type": "UInt32"}], "data": [{"n_24h": 3}]}},
            "atlas": {"type": "Topology", "objects": {}, "arcs": []}}
    p = tmp_path / "snap.json"
    p.write_text(__import__("json").dumps(snap))
    return p


def test_default_build_carries_no_snapshot(built):
    html, _ = built
    assert "SNAPSHOT" not in html


def test_snapshot_build_answers_fetches_from_the_snapshot(tmp_path):
    out = tmp_path / "t.html"
    subprocess.run([sys.executable, BUILD, "--snapshot", str(_snapshot(tmp_path)), str(out)],
                   check=True, capture_output=True)
    html = out.read_text()
    assert "const SNAPSHOT = " in html and '"n_24h": 3' in html
    assert html.index("const SNAPSHOT = ") < html.index("function parseHash")   # shim runs first
    assert '"Snapshot of " + SNAPSHOT.taken_utc' in html and '"taken_utc": "2026-11-14T03:10Z"' in html


def test_snapshot_must_be_a_snapshot(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text("{}")
    run = subprocess.run([sys.executable, BUILD, "--snapshot", str(bad), str(tmp_path / "t.html")],
                         capture_output=True, text=True)
    assert run.returncode != 0
