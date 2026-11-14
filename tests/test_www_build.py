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
