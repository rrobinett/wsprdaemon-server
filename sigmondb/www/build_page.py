"""Build the public transmitter map as one HTML file for a Google Sites embed block.

    python3 sigmondb/www/build_page.py [--standalone-url URL] [OUT]

Replaces the marker <!-- LOGIC --> in page.html with logic.js inside a <script> element,
and sets the page's STANDALONE_URL constant (empty unless --standalone-url gives one).
Inside a frame, such as a Google Sites embed, the visitor's address bar never shows the
page's fragment; STANDALONE_URL is where the page also lives on its own, so the framed
page can offer a link to the current view there.  Empty means the page offers none.
OUT defaults to sigmondb/www/dist/transmitter-map.html.  Prints the output's size in bytes.
"""
import argparse
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
MARKER = "<!-- LOGIC -->"
URL_SLOT = 'const STANDALONE_URL = "";'


def build(out_path, standalone_url=""):
    page = open(os.path.join(HERE, "page.html"), encoding="utf-8").read()
    logic = open(os.path.join(HERE, "logic.js"), encoding="utf-8").read()
    if page.count(MARKER) != 1:
        raise SystemExit(f"page.html must hold the marker {MARKER} exactly once")
    if page.count(URL_SLOT) != 1:
        raise SystemExit(f"page.html must hold {URL_SLOT} exactly once")
    if "</script" in logic:
        raise SystemExit("logic.js holds '</script', which would end the inlined element early")
    if standalone_url and not re.fullmatch(r"https://[^\s<>\"'#]+", standalone_url):
        raise SystemExit("--standalone-url must be an https URL with no spaces, quotes, angle brackets or '#'")
    html = page.replace(URL_SLOT, f"const STANDALONE_URL = {json.dumps(standalone_url)};")
    html = html.replace(MARKER, "<script>\n" + logic + "</script>")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    data = html.encode("utf-8")
    with open(out_path, "wb") as f:
        f.write(data)
    return len(data)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out", nargs="?", default=os.path.join(HERE, "dist", "transmitter-map.html"))
    ap.add_argument("--standalone-url", default="", help="https URL where the page also lives unframed")
    args = ap.parse_args()
    size = build(args.out, args.standalone_url)
    print(f"{args.out}: {size} bytes")


if __name__ == "__main__":
    main()
