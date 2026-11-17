"""Build the public fleet board as one HTML file.

    python3 fleetboard/www/build_page.py [OUT]

Replaces the marker <!-- LOGIC --> in page.html with logic.js inside a <script> element.
OUT defaults to fleetboard/www/dist/fleetboard.html.  Prints the output's size in bytes.
"""
import argparse
import os

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


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out", nargs="?", default=os.path.join(HERE, "dist", "fleetboard.html"))
    args = ap.parse_args()
    print(f"{args.out}: {build(args.out)} bytes")


if __name__ == "__main__":
    main()
