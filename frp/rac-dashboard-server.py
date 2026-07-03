#!/usr/bin/env python3
"""rac-dashboard — tier-aware live Sigmond RAC services page + history, on gw2.

A background thread polls the frps admin API every 30s and records each client's
connection *sessions* (connect -> disconnect) in a small SQLite DB. The web view
(per request) shows a table sorted by name with a Status column (active / last
seen), plus a per-client history sub-page (?h=<name>).

Tier-aware: the WireGuard address the request arrived on selects the view
(10.112.0.2 mesh = full, 10.111.220.1 wd-rac = shareable services only); any
non-WireGuard interface is refused.
"""
import base64
import html
import json
import os
import sqlite3
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# Both frps instances: the secure gateway (:35736, API :7501) and the legacy
# open gateway (:35735, API :7500). Legacy clients get a ' [legacy]' name tag
# so a station that exists in both worlds (old autossh-era client + new
# appliance under the same callsign) keeps two distinct rows and histories.
# As a bonus, polling the open server means the history DB now accumulates
# an ever-seen record of legacy stations, which frps itself forgets on
# every restart.
FRPS_APIS  = [
    ('secure', 'http://127.0.0.1:7501/api/proxy/tcp', 'admin:admin'),
    ('legacy', 'http://127.0.0.1:7500/api/proxy/tcp', 'admin:admin'),
]
PORT       = 50080
MESH_ADDR  = '10.112.0.2'
RAC_ADDR   = '10.111.220.1'
DB_PATH    = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rac-history.db')
POLL_SEC   = 30
RETAIN_SEC = 180 * 86400          # prune closed sessions older than ~6 months

SUFFIXES = [
    ('-host-ssh', 'ssh', 'ssh',   'mesh-only', 'host SSH'),
    ('-host-ui',  'web', 'https', 'mesh-only', 'Proxmox UI'),
    ('-vm-ssh',   'ssh', 'ssh',   'mesh-only', 'VM SSH'),
    ('-vm-web',   'web', 'http',  'shareable', 'VM ka9q-web'),
    ('-ssh',      'ssh', 'ssh',   'mesh-only', 'SSH'),        # receiver (smd)
    ('-web',      'web', 'http',  'shareable', 'ka9q-web'),   # receiver (smd)
]
COLUMNS = [
    ('VM Web',          ['VM ka9q-web', 'ka9q-web'], 'web'),
    ('VM SSH',          ['VM SSH', 'SSH'],           'ssh'),
    ('Proxmox UI',      ['Proxmox UI'],              'web'),
    ('Proxmox box SSH', ['host SSH'],                'ssh'),
]

STYLE = (
    'body{font-family:system-ui,-apple-system,sans-serif;max-width:1000px;'
    'margin:2rem auto;padding:0 1rem;color:#222}h1{font-size:1.4rem;margin-bottom:.2rem}'
    '.sub{color:#777;font-size:.85rem;margin-bottom:1rem}'
    'table{border-collapse:collapse;width:100%;font-size:.92rem}'
    'th,td{text-align:left;padding:.35rem .8rem;border-bottom:1px solid #ececec;white-space:nowrap}'
    'th{font-size:.74rem;text-transform:uppercase;letter-spacing:.04em;color:#999;'
    'border-bottom:2px solid #ddd}th.svc,td.svc{text-align:center}'
    'td.name{font-family:ui-monospace,Menlo,monospace;font-weight:600}'
    'a{color:#1a6dd6;text-decoration:none}a:hover{text-decoration:underline}'
    'code{background:#f4f4f4;padding:.1rem .35rem;border-radius:4px;font-size:.82rem;color:#555}'
    '.dot{color:#ccc}.on{color:#28a745;font-weight:600}.off-row td{opacity:.5}'
    '.ago{color:#999;font-size:.85rem}tr:hover td{background:#fafafa}'
    '.act{text-decoration:underline;cursor:pointer}')

# Clipboard copy that works over plain HTTP (navigator.clipboard needs HTTPS):
# a transient textarea + execCommand, with a brief "copied" confirmation.
SCRIPT = ("<script>function cp(el,t){var a=document.createElement('textarea');"
          "a.value=t;a.style.position='fixed';a.style.opacity=0;"
          "document.body.appendChild(a);a.focus();a.select();"
          "try{document.execCommand('copy');}catch(e){}document.body.removeChild(a);"
          "var o=el.textContent;el.textContent='copied';"
          "setTimeout(function(){el.textContent=o;},900);}</script>")


# --------------------------------------------------------------------------- DB
def db():
    con = sqlite3.connect(DB_PATH, timeout=5)
    con.execute('PRAGMA journal_mode=WAL')
    return con


def init_db():
    con = db()
    con.execute('''CREATE TABLE IF NOT EXISTS sessions(
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        connected_at INTEGER NOT NULL,
        last_seen INTEGER NOT NULL,
        disconnected_at INTEGER)''')
    con.execute('CREATE INDEX IF NOT EXISTS idx_sess_name ON sessions(name)')
    con.commit()
    con.close()


def record(online, now):
    con = db()
    c = con.cursor()
    for name in online:
        row = c.execute('SELECT id FROM sessions WHERE name=? AND disconnected_at IS NULL',
                        (name,)).fetchone()
        if row:
            c.execute('UPDATE sessions SET last_seen=? WHERE id=?', (now, row[0]))
        else:
            c.execute('INSERT INTO sessions(name,connected_at,last_seen) VALUES(?,?,?)',
                      (name, now, now))
    for sid, name, last_seen in c.execute(
            'SELECT id,name,last_seen FROM sessions WHERE disconnected_at IS NULL').fetchall():
        if name not in online:
            c.execute('UPDATE sessions SET disconnected_at=? WHERE id=?', (last_seen, sid))
    c.execute('DELETE FROM sessions WHERE disconnected_at IS NOT NULL AND disconnected_at<?',
              (now - RETAIN_SEC,))
    con.commit()
    con.close()


def status_map():
    con = db()
    rows = con.execute('''SELECT name,
        MAX(disconnected_at IS NULL) AS active,
        MAX(last_seen) AS last_seen,
        MIN(connected_at) AS first_seen,
        COUNT(*) AS sessions
        FROM sessions GROUP BY name''').fetchall()
    con.close()
    return {r[0]: {'active': bool(r[1]), 'last_seen': r[2],
                   'first_seen': r[3], 'sessions': r[4]} for r in rows}


def history(name):
    con = db()
    rows = con.execute('''SELECT connected_at,disconnected_at,last_seen FROM sessions
        WHERE name=? ORDER BY connected_at DESC LIMIT 500''', (name,)).fetchall()
    con.close()
    return rows


# ----------------------------------------------------------------------- poller
def fetch_proxies():
    out = []
    for src, url, auth in FRPS_APIS:
        try:
            req = urllib.request.Request(url)
            req.add_header('Authorization',
                           'Basic ' + base64.b64encode(auth.encode()).decode())
            with urllib.request.urlopen(req, timeout=8) as r:
                for p in json.load(r).get('proxies') or []:
                    p['_src'] = src
                    out.append(p)
        except Exception:
            pass  # one instance being down must not blank the whole page
    return out


def strip_hash(name):
    if '.' in name:
        head, rest = name.split('.', 1)
        if len(head) == 16 and all(c in '0123456789abcdef' for c in head):
            return rest
    return name


def parse(name):
    for suf, kind, proto, tier, label in SUFFIXES:
        if name.endswith(suf):
            return name[:-len(suf)], kind, proto, tier, label
    if name.endswith('-WEB'):
        client = name[:-4]
        if 'PROXMOX' in client.upper():
            return client, 'web', 'https', 'mesh-only', 'Proxmox UI'
        return client, 'web', 'http', 'shareable', 'ka9q-web'
    return name, 'ssh', 'ssh', 'mesh-only', 'SSH'


def live_services():
    """{client: {label: svc}} for currently-online proxies, + set of online clients."""
    clients = {}
    online = set()
    for p in fetch_proxies():
        name = strip_hash(p.get('name', ''))
        if not name or p.get('status') != 'online':
            continue
        client, kind, proto, tier, label = parse(name)
        if p.get('_src') == 'legacy':
            client += ' [legacy]'
        online.add(client)
        conf = p.get('conf') or {}
        clients.setdefault(client, {})[label] = {
            'kind': kind, 'proto': proto, 'tier': tier, 'label': label,
            'port': conf.get('remotePort'),
            'user': (conf.get('metadatas') or {}).get('user')}
    return clients, online


def poller():
    while True:
        try:
            _clients, online = live_services()
            record(online, int(time.time()))
        except Exception:
            pass
        time.sleep(POLL_SEC)


# ------------------------------------------------------------------ formatting
def ago(ts, now):
    d = max(0, now - ts)
    if d < 60:
        return f'{d}s ago'
    if d < 3600:
        return f'{d // 60}m ago'
    if d < 86400:
        return f'{d // 3600}h ago'
    return f'{d // 86400}d ago'


def utc(ts):
    return time.strftime('%Y-%m-%d %H:%M', time.gmtime(ts)) + 'Z'


def dur(a, b):
    s = max(0, b - a)
    if s < 3600:
        return f'{s // 60}m'
    if s < 86400:
        return f'{s // 3600}h {s % 3600 // 60}m'
    return f'{s // 86400}d {s % 86400 // 3600}h'


# --------------------------------------------------------------------- rendering
def page(title, body):
    return ('<!doctype html><html><head><meta charset="utf-8">'
            '<title>' + html.escape(title) + '</title>'
            '<meta http-equiv="refresh" content="30">'
            '<style>' + STYLE + '</style>' + SCRIPT +
            '</head><body>' + body + '</body></html>')


def render_main(gw_ip, tier):
    now = int(time.time())
    clients, _online = live_services()
    stat = status_map()
    # tier-filter live services
    vis = {}
    for name, svcs in clients.items():
        f = {lab: s for lab, s in svcs.items()
             if not (tier == 'rac' and s['tier'] == 'mesh-only')}
        if f:
            vis[name] = f
    present = [(h, ls, k) for (h, ls, k) in COLUMNS
               if any(any(l in vis[c] for l in ls) for c in vis)]
    names = sorted(set(stat) | set(vis))

    b = ['<h1>Sigmond Remote Access Channel (RAC)</h1>']
    tl = 'full mesh view' if tier == 'mesh' else 'wd-rac view (shareable services only)'
    nact = sum(1 for n in names if stat.get(n, {}).get('active') or n in vis)
    b.append(f'<div class="sub">via gw2 <code>{gw_ip}</code> &middot; {tl} &middot; '
             f'{nact} active / {len(names)} known &middot; auto-refresh 30s</div>')
    b.append('<table><tr><th>Name</th><th>Status</th>')
    for h, _ls, _k in present:
        b.append(f'<th class="svc">{h}</th>')
    b.append('</tr>')
    for name in names:
        active = stat.get(name, {}).get('active') or name in vis
        ls = stat.get(name, {}).get('last_seen')
        rowcls = '' if active else ' class="off-row"'
        st = '<span class="on">&#9679; active</span>' if active else (
            f'<span class="ago">last seen {ago(ls, now)}</span>' if ls else '&mdash;')
        b.append(f'<tr{rowcls}><td class="name">'
                 f'<a href="?h={html.escape(name)}">{html.escape(name)}</a></td>'
                 f'<td>{st}</td>')
        svcs = vis.get(name, {})
        for _h, labels, kind in present:
            svc = next((svcs[l] for l in labels if l in svcs), None)
            if not svc:
                b.append('<td class="svc"><span class="dot">&middot;</span></td>')
            elif kind == 'web':
                b.append('<td class="svc"><a class="act" target="_blank" '
                         'href="%s://%s:%s/">open</a></td>'
                         % (svc['proto'], gw_ip, svc['port']))
            else:
                cmd = 'ssh -p %s %s@%s' % (svc['port'], svc.get('user') or 'root', gw_ip)
                b.append('<td class="svc"><a class="act" href="#" title="%s" '
                         "onclick=\"cp(this,'%s');return false\">ssh</a></td>"
                         % (cmd, cmd))
        b.append('</tr>')
    b.append('</table>')
    return page('Sigmond RAC', ''.join(b))


def render_history(name, gw_ip):
    now = int(time.time())
    rows = history(name)
    b = [f'<h1>{html.escape(name)} &mdash; connection history</h1>',
         '<div class="sub"><a href="/">&larr; all machines</a> &middot; '
         f'{len(rows)} session(s) &middot; via gw2 <code>{gw_ip}</code></div>']
    if not rows:
        b.append('<p>No history recorded yet.</p>')
        return page(name + ' history', ''.join(b))
    b.append('<table><tr><th>Connected</th><th>Disconnected</th><th>Duration</th></tr>')
    for connected_at, disconnected_at, last_seen in rows:
        if disconnected_at is None:
            end = '<span class="on">&#9679; active</span>'
            d = dur(connected_at, now)
        else:
            end = f'{utc(disconnected_at)} <span class="ago">({ago(disconnected_at, now)})</span>'
            d = dur(connected_at, disconnected_at)
        b.append(f'<tr><td>{utc(connected_at)}</td><td>{end}</td><td>{d}</td></tr>')
    b.append('</table>')
    return page(name + ' history', ''.join(b))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        local = self.connection.getsockname()[0]
        if local == MESH_ADDR:
            tier = 'mesh'
        elif local == RAC_ADDR:
            tier = 'rac'
        else:
            self.send_response(403)
            self.end_headers()
            self.wfile.write(b'forbidden: WireGuard admin networks only')
            return
        try:
            q = parse_qs(urlparse(self.path).query)
            if 'h' in q and q['h']:
                body = render_history(q['h'][0], local)
            else:
                body = render_main(local, tier)
            page_b = body.encode()
        except Exception as exc:
            self.send_response(502)
            self.end_headers()
            self.wfile.write(('error: %s' % exc).encode())
            return
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', str(len(page_b)))
        self.end_headers()
        self.wfile.write(page_b)

    def log_message(self, *a):
        pass


if __name__ == '__main__':
    init_db()
    threading.Thread(target=poller, daemon=True).start()
    ThreadingHTTPServer(('0.0.0.0', PORT), Handler).serve_forever()
