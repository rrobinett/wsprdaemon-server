"""Build the public transmitter map as one HTML file for a Google Sites embed block.

    python3 sigmondb/www/build_page.py [OUT]

Replaces the marker <!-- LOGIC --> in page.html with logic.js inside a <script> element.
OUT defaults to sigmondb/www/dist/transmitter-map.html.  Prints the output's size in bytes.
"""
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
MARKER = "<!-- LOGIC -->"


def build(out_path):
    page = open(os.path.join(HERE, "page.html"), encoding="utf-8").read()
    logic = open(os.path.join(HERE, "logic.js"), encoding="utf-8").read()
    if page.count(MARKER) != 1:
        raise SystemExit(f"page.html must hold the marker {MARKER} exactly once")
    if "</script" in logic:
        raise SystemExit("logic.js holds '</script', which would end the inlined element early")
    html = page.replace(MARKER, "<script>\n" + logic + "</script>")
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    data = html.encode("utf-8")
    with open(out_path, "wb") as f:
        f.write(data)
    return len(data)


def main(argv):
    out = argv[1] if len(argv) > 1 else os.path.join(HERE, "dist", "transmitter-map.html")
    size = build(out)
    print(f"{out}: {size} bytes")


if __name__ == "__main__":
    main(sys.argv)
