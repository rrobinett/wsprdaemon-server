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


# Review copies only.  A claude.ai artifact runs in a sandbox that refuses fetches to outside
# servers, so the live page there shows every endpoint unreachable.  --snapshot puts this shim
# ahead of everything else: it answers the page's table and atlas fetches from a snapshot taken
# at build time and says so on the page.  The published build never carries it.
SNAPSHOT_SHIM = """<script>
const SNAPSHOT = %s;
(function () {
  const live = window.fetch ? window.fetch.bind(window) : null;
  const answer = body => Promise.resolve({ok: true, status: 200,
    headers: {get: () => null}, json: () => Promise.resolve(body)});
  window.fetch = function (url, opts) {
    const u = String(url);
    const t = /sigmondb_public\\.([a-z_0-9]+)/.exec(decodeURIComponent(u));
    if (t) return SNAPSHOT.tables[t[1]] ? answer(SNAPSHOT.tables[t[1]])
                                        : Promise.resolve({ok: false, status: 404,
                                            headers: {get: () => "60"}, json: () => Promise.resolve({})});
    if (/states-10m\\.json/.test(u)) return answer(SNAPSHOT.atlas);
    return live ? live(url, opts) : Promise.reject(new Error("no fetch"));
  };
  document.addEventListener("DOMContentLoaded", function () {
    const b = document.createElement("div");
    b.setAttribute("role", "note");
    b.style.cssText = "padding:8px 16px;font:13px/1.4 system-ui,sans-serif;background:#fff4d6;color:#3d3000;border-bottom:1px solid #e0c97a";
    b.textContent = "Snapshot of " + SNAPSHOT.taken_utc + " for review. The published page reads the public servers live.";
    document.body.insertBefore(b, document.body.firstChild);
  });
})();
</script>
"""


def build(out_path, standalone_url="", snapshot=None):
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
    if snapshot is not None:
        if not {"taken_utc", "tables", "atlas"} <= set(snapshot):
            raise SystemExit("--snapshot needs taken_utc, tables and atlas")
        body = json.dumps(snapshot, indent=1).replace("</", "<\\/")
        html = SNAPSHOT_SHIM % body + html
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    data = html.encode("utf-8")
    with open(out_path, "wb") as f:
        f.write(data)
    return len(data)


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("out", nargs="?", default=os.path.join(HERE, "dist", "transmitter-map.html"))
    ap.add_argument("--standalone-url", default="", help="https URL where the page also lives unframed")
    ap.add_argument("--snapshot", help="review copies only: JSON {taken_utc, tables, atlas} the page "
                                       "answers its fetches from (see SNAPSHOT_SHIM)")
    args = ap.parse_args()
    snap = json.load(open(args.snapshot, encoding="utf-8")) if args.snapshot else None
    size = build(args.out, args.standalone_url, snap)
    print(f"{args.out}: {size} bytes")


if __name__ == "__main__":
    main()
