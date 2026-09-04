#!/usr/bin/env python3
"""
rac-registrar.py — self-service RAC registration for the frps-secure gateway.

A Sigmond appliance (or smd receiver) POSTs its SSH public key and its
admin-assigned RAC number.  This service does what the admin used to do
by hand:

  1. creates the per-station gateway account (/home/<SITE>, shell /bin/false),
  2. installs the pubkey in that account's authorized_keys — the registry
     frps-auth-plugin.py validates frpc logins against,
  3. records the RAC→site claim (first come, first served) in
     /home/frp/rac-registry.json,
  4. returns everything the client needs to render its frpc config: the
     frps `user` (first 16 hex of sha256 of the pubkey line), the fleet
     auth token, and the deterministic ports for RAC n:

         vm-ssh   35800+n     (the decoder VM's sshd)
         vm-web   45800+n     (the decoder VM's ka9q-web)
         host-ssh 50800+n     (the Proxmox host's sshd)
         host-ui  55800+n     (the Proxmox web UI, https :8006)
         vm-grape 40800+n     (WsprDaemon GRAPE carrier strip-chart page, http :8088; WD 3.4.6+)

     All four bands stay inside the wd-rac WireGuard tier's allowed
     35800-59999 range for RAC 0-999.  Suffixes match rac-dashboard's
     SUFFIXES table, so registered stations appear there automatically.

The fleet token is intentionally returned to callers: it is already a
published constant in the public upstream smd, and the REAL gate on
frps-secure is the auth plugin's pubkey check — an unregistered key
cannot log in no matter what token it presents.  Registration is
logged (journal + registry file) and revocable: delete the key line
from /home/<SITE>/.ssh/authorized_keys (takes effect on next login).

API (JSON in/out):
  POST /register  {"site": "AI6VN_151", "rac": 151,      # rac optional —
                   "pubkey": "ssh-ed25519 AAAA..."}      # omit to auto-assign
      Auto-assignment picks the lowest free number >= 500 after checking
      the registry, both live frps instances, WireGuard peers, the
      cumulative ever-seen file and the admin's rac-reserved.txt (see the
      auto-assignment block below). A site that is already registered
      always gets its existing number back (sticky).
    200 -> {"ok": true, "site": ..., "rac": ..., "user": "<16hex>",
            "token": "<fleet token>", "server_addr": "gw2.wsprdaemon.org",
            "server_port": 35736,
            "gateways": [{"name": "gw2", "addr": ..., "port": 35736, "role": "primary"},
                         {"name": "gw1", "addr": ..., "port": 35736, "role": "standby"}],
            "ports": {"vm_ssh":..., "vm_web":..., "host_ssh":..., "host_ui":...}}
      Dual-tunnel clients open one frpc per entry in "gateways";
      server_addr/server_port stay for installed single-tunnel clients.
    400 bad input | 409 RAC or site already claimed by someone else | 500
  POST /register-sonde {"site": ..., "pubkey": ...}
      Wsprsonde namespace (wsprsonde-rac-client): random sticky ID from
      2000-2099, ssh port 35800+ID (37800-37899). Response mirrors
      /register plus "admin_keys": operator pubkeys for the node's
      'wsprsonde' account, from /home/frp/sonde-admin-keys.txt.
  GET /health -> {"ok": true}

Idempotent: re-registering the same site+rac (same or new key) succeeds,
so `sigmond-setup --reconfigure` just works.

Runs as root (needs useradd).  Unit: frp/rac-registrar.service.
"""

import argparse
import fcntl
import hashlib
import json
import logging
import os
import pwd
import random
import re
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = "1.4.0"

# After every successful registration, push this gateway's state to the
# standby right away (rac-failover-sync.service; the hourly timer is the
# backstop). Dual-tunnel clients start their standby instance seconds after
# registering, and gw1 can only accept them once it has the new account+key.
FAILOVER_SYNC_UNIT = "rac-failover-sync.service"


def kick_failover_sync():
    try:
        subprocess.Popen(["systemctl", "start", "--no-block", FAILOVER_SYNC_UNIT],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:  # never let replication trouble break a registration
        log.warning("could not start %s: %s", FAILOVER_SYNC_UNIT, e)

FRPS_TOML = "/home/frp/frps-secure.toml"
REGISTRY = "/home/frp/rac-registry.json"
SERVER_ADDR = "gw2.wsprdaemon.org"
SERVER_PORT = 35736

# Every gateway a client should hold a tunnel to, primary first. Returned as
# "gateways" alongside the legacy single server_addr/server_port (kept
# verbatim so installed single-tunnel clients are untouched). A station's
# identity -- pubkey hash, fleet token, port band -- is the same on every
# gateway; the gw2->gw1 failover replication (sync-failover-state.sh) is
# what guarantees that, so a client can arrive at the standby unannounced.
GATEWAYS = [
    {"name": "gw2", "addr": SERVER_ADDR, "port": SERVER_PORT, "role": "primary"},
    {"name": "gw1", "addr": "gw1.wsprdaemon.org", "port": 35736, "role": "standby"},
]

BANDS = {"vm_ssh": 35800, "vm_web": 45800, "host_ssh": 50800, "host_ui": 55800, "vm_grape": 40800}

# ── auto-assignment ─────────────────────────────────────────────────────────
# Stations are identified by reporter ID; the RAC number is plumbing — so
# clients may omit "rac" and let the gateway pick. There is NO complete
# record of every RAC the legacy fleet ever used (frps forgets proxies on
# restart, the dashboard db keeps names not ports, and offline legacy
# stations can reappear), so auto-assignment defends in depth:
#   1. floor 500 — the observed legacy fleet lives in 0-199, so auto picks
#      can't land on a sleeping legacy station's number;
#   2. skip everything visible RIGHT NOW on both frps instances (secure
#      :7501 and the legacy open server :7500), mapped port→rac;
#   3. skip WireGuard wd-rac peers (10.111.220.(10+rac) for rac 0-199);
#   4. skip RAC_SEEN — a cumulative ever-seen file this service grows from
#      the live views on every request;
#   5. skip RAC_RESERVED — admin-maintained, one rac per line, for known
#      offline legacy stations (comments with # allowed).
# frps itself is the final arbiter: a port already bound just fails at
# frpc connect time, which the wizard's channel verification surfaces.
AUTO_FLOOR = 500
RAC_SEEN = "/home/frp/rac-seen.json"
RAC_RESERVED = "/home/frp/rac-reserved.txt"
WG_CONF = "/etc/wireguard/wd-rac.conf"
FRPS_APIS = [
    ("secure", "http://127.0.0.1:7501/api/proxy/tcp", "admin:admin"),
    ("open",   "http://127.0.0.1:7500/api/proxy/tcp", "admin:admin"),
]

# ── Wsprsonde namespace ─────────────────────────────────────────────────────
# Wsprsonde hosts (wsprsonde-rac-client) register at POST /register-sonde and
# live in their own ID space, 2000-2099, deliberately outside the 0-999 RAC
# range. The ssh port keeps the 35800+n formula, giving 37800-37899 — a free
# gap between the vm-ssh (35800-36799) and vm-web (45800+) bands, still inside
# the wd-rac WireGuard tier's allowed 35800-59999 range. IDs are assigned at
# RANDOM from the free pool (sticky per site), and the registrar returns the
# operator ssh keys from SONDE_ADMIN_KEYS for the node to install in its
# 'wsprsonde' account.
SONDE_REGISTRY = "/home/frp/sonde-registry.json"
SONDE_ADMIN_KEYS = "/home/frp/sonde-admin-keys.txt"
SONDE_MIN, SONDE_MAX = 2000, 2099

SITE_RE = re.compile(r"^[A-Z0-9][A-Z0-9_-]{2,31}$")
PUBKEY_RE = re.compile(
    r"^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp(256|384|521)) [A-Za-z0-9+/=]+( \S+)?$"
)

log = logging.getLogger("rac-registrar")


def fleet_token():
    with open(FRPS_TOML) as f:
        for line in f:
            m = re.match(r'\s*token\s*=\s*"([^"]+)"', line)
            if m:
                return m.group(1)
    raise RuntimeError("no token found in %s" % FRPS_TOML)


def key_user(pubkey_line):
    """frps user = first 16 hex of sha256(authorized_keys line + newline),
    matching both frps-auth-plugin.py and `sha256sum id_ed25519.pub`."""
    return hashlib.sha256((pubkey_line + "\n").encode()).hexdigest()[:16]


def valid_rac(rac):
    # Mirror add-rac-client.sh: 200-299 is the HamSCI range, not WD's.
    return isinstance(rac, int) and 0 <= rac <= 999 and not 200 <= rac <= 299


def port_to_rac(port):
    """Reverse-map any known band (incl. the legacy 35800/45800 ssh/web
    bands, which share bases with vm_ssh/vm_web) back to a rac number."""
    for base in (35800, 40800, 45800, 50800, 55800):
        if base <= port <= base + 999:
            return port - base
    return None


def live_racs():
    """rac numbers implied by every proxy currently known to either frps
    instance. Unreachable APIs are logged and skipped (floor + frps port
    arbitration still protect us)."""
    import base64
    import urllib.request
    racs = set()
    for label, url, auth in FRPS_APIS:
        try:
            req = urllib.request.Request(url)
            req.add_header("Authorization",
                           "Basic " + base64.b64encode(auth.encode()).decode())
            data = json.load(urllib.request.urlopen(req, timeout=5))
            for p in data.get("proxies") or []:
                port = (p.get("conf") or {}).get("remotePort")
                r = port_to_rac(port) if isinstance(port, int) else None
                if r is not None:
                    racs.add(r)
        except Exception as e:
            log.warning("frps %s API unavailable (%s) — skipping", label, e)
    return racs


def wg_racs():
    """rac numbers implied by wd-rac WireGuard peers:
    add-rac-client.sh maps rac 0-199 -> 10.111.220.(10+rac)."""
    racs = set()
    try:
        with open(WG_CONF) as f:
            for m in re.finditer(r"AllowedIPs\s*=\s*10\.111\.220\.(\d+)/32", f.read()):
                octet = int(m.group(1))
                if 10 <= octet <= 209:
                    racs.add(octet - 10)
    except OSError:
        pass
    return racs


def reserved_racs():
    """Union of every reservation source; also grows the cumulative
    ever-seen file from the current live view."""
    racs = set()
    if os.path.exists(REGISTRY):
        with open(REGISTRY) as f:
            racs |= {int(k) for k in json.load(f)}
    seen = set()
    if os.path.exists(RAC_SEEN):
        with open(RAC_SEEN) as f:
            seen = set(json.load(f))
    live = live_racs()
    if not live <= seen:
        tmp = RAC_SEEN + ".tmp"
        with open(tmp, "w") as f:
            json.dump(sorted(seen | live), f)
        os.replace(tmp, RAC_SEEN)
    racs |= seen | live | wg_racs()
    if os.path.exists(RAC_RESERVED):
        with open(RAC_RESERVED) as f:
            for line in f:
                line = line.split("#")[0].strip()
                if line.isdigit():
                    racs.add(int(line))
    return racs


def pick_free_rac():
    taken = reserved_racs()
    for n in range(AUTO_FLOOR, 1000):
        if n not in taken:
            return n
    return None


def ensure_account(site, pubkey_line):
    try:
        pwd.getpwnam(site)
    except KeyError:
        subprocess.run(
            ["useradd", "-m", "-s", "/bin/false", site], check=True, capture_output=True
        )
        log.info("created account %s", site)
    home = pwd.getpwnam(site).pw_dir
    sshdir = os.path.join(home, ".ssh")
    akf = os.path.join(sshdir, "authorized_keys")
    os.makedirs(sshdir, mode=0o700, exist_ok=True)
    existing = ""
    if os.path.exists(akf):
        with open(akf) as f:
            existing = f.read()
    # match on key type+blob so a changed trailing comment still dedups
    blob = " ".join(pubkey_line.split()[:2])
    if not any(l.strip().startswith(blob) for l in existing.splitlines()):
        with open(akf, "a") as f:
            if existing and not existing.endswith("\n"):
                f.write("\n")
            f.write(pubkey_line + "\n")
        log.info("added key %s to %s", key_user(pubkey_line), akf)
    u = pwd.getpwnam(site)
    os.chown(sshdir, u.pw_uid, u.pw_gid)
    os.chown(akf, u.pw_uid, u.pw_gid)
    os.chmod(akf, 0o600)


def claim(site, rac, user, registry=REGISTRY):
    """First-come-first-served RAC→site claim, atomically under a lock."""
    with open(registry + ".lock", "w") as lk:
        fcntl.flock(lk, fcntl.LOCK_EX)
        reg = {}
        if os.path.exists(registry):
            with open(registry) as f:
                reg = json.load(f)
        rk = str(rac)
        ent = reg.get(rk)
        if ent and ent["site"] != site:
            if user in ent.get("users", []):
                # Same node key, new name: the station was renamed (e.g. a
                # WsprDaemon host now registering under its reporter ID after
                # a manual registration). Keep the RAC, take the new name.
                log.info("RAC %d: site renamed %s -> %s (same node key %s)", rac, ent["site"], site, user)
                ent["site"] = site
            else:
                return "RAC %d is already registered to site %s" % (rac, ent["site"])
        for other_rk, other in reg.items():
            if other["site"] == site and other_rk != rk:
                return "site %s is already registered as RAC %s" % (site, other_rk)
        if ent is None:
            ent = {"site": site, "users": [], "created": time.strftime("%F %T")}
        if user not in ent["users"]:
            ent["users"].append(user)
        ent["updated"] = time.strftime("%F %T")
        reg[rk] = ent
        tmp = registry + ".tmp"
        with open(tmp, "w") as f:
            json.dump(reg, f, indent=2, sort_keys=True)
        os.replace(tmp, registry)
    return None


# ── Wsprsonde helpers ───────────────────────────────────────────────────────
def live_sonde_ids():
    """Sonde IDs implied by proxies currently on either frps instance
    (remotePort 37800-37899 -> ID 2000-2099)."""
    import base64
    import urllib.request
    ids = set()
    for label, url, auth in FRPS_APIS:
        try:
            req = urllib.request.Request(url)
            req.add_header("Authorization",
                           "Basic " + base64.b64encode(auth.encode()).decode())
            data = json.load(urllib.request.urlopen(req, timeout=5))
            for p in data.get("proxies") or []:
                port = (p.get("conf") or {}).get("remotePort")
                if isinstance(port, int) and 37800 <= port <= 37899:
                    ids.add(port - 35800)
        except Exception as e:
            log.warning("frps %s API unavailable (%s) — skipping", label, e)
    return ids


def pick_free_sonde():
    """Random free sonde ID: registry claims + live tunnels are both taken."""
    taken = live_sonde_ids()
    if os.path.exists(SONDE_REGISTRY):
        with open(SONDE_REGISTRY) as f:
            taken |= {int(k) for k in json.load(f)}
    free = [n for n in range(SONDE_MIN, SONDE_MAX + 1) if n not in taken]
    return random.choice(free) if free else None


def sonde_admin_keys():
    """Operator ssh public keys the node should install in its 'wsprsonde'
    account, one per line in SONDE_ADMIN_KEYS (# comments allowed)."""
    keys = []
    try:
        with open(SONDE_ADMIN_KEYS) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#"):
                    keys.append(line)
    except OSError:
        pass
    return keys


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "rac-registrar/" + VERSION
    _last_by_ip = {}

    def log_message(self, fmt, *args):
        log.info("%s " + fmt, self.client_address[0], *args)

    def _reply(self, code, obj):
        body = (json.dumps(obj) + "\n").encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._reply(200, {"ok": True, "version": VERSION})
        else:
            self._reply(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path == "/register-sonde":
            self._register_sonde()
            return
        if self.path != "/register":
            self._reply(404, {"ok": False, "error": "not found"})
            return
        ip = self.client_address[0]
        now = time.monotonic()
        if now - self._last_by_ip.get(ip, -60) < 5:
            self._reply(429, {"ok": False, "error": "slow down"})
            return
        self._last_by_ip[ip] = now
        try:
            length = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(min(length, 65536)))
            site = str(req.get("site", "")).strip().upper()
            rac = req.get("rac")
            pubkey = " ".join(str(req.get("pubkey", "")).split())
        except (ValueError, TypeError):
            self._reply(400, {"ok": False, "error": "bad JSON"})
            return
        if not SITE_RE.match(site):
            self._reply(400, {"ok": False, "error": "bad site name (A-Z 0-9 _ -, 3-32 chars)"})
            return
        auto = rac in (None, "", "auto")
        if not auto and not valid_rac(rac):
            self._reply(400, {"ok": False, "error": "RAC must be 0-199 or 300-999 (or omit it to auto-assign)"})
            return
        if not PUBKEY_RE.match(pubkey):
            self._reply(400, {"ok": False, "error": "pubkey doesn't look like an OpenSSH public key line"})
            return

        # Sticky: a site that's already registered keeps its number — this
        # is what makes `sigmond-setup --reconfigure` idempotent.
        existing = None
        if os.path.exists(REGISTRY):
            with open(REGISTRY) as f:
                for rk, ent in json.load(f).items():
                    if ent["site"] == site:
                        existing = int(rk)
        if existing is not None:
            if not auto and rac != existing:
                self._reply(409, {"ok": False, "error":
                    "site %s is already registered as RAC %d" % (site, existing)})
                return
            rac = existing
        elif auto:
            rac = pick_free_rac()
            if rac is None:
                log.error("auto-assign: no free RAC left in %d-999", AUTO_FLOOR)
                self._reply(503, {"ok": False, "error": "no free RAC numbers left — contact the admin"})
                return
            log.info("auto-assigned RAC %d to site %s", rac, site)

        user = key_user(pubkey)
        err = claim(site, rac, user)
        if err:
            log.warning("REJECTED %s rac=%s site=%s: %s", ip, rac, site, err)
            self._reply(409, {"ok": False, "error": err})
            return
        try:
            ensure_account(site, pubkey)
        except Exception as e:
            log.error("account setup failed for %s: %s", site, e)
            self._reply(500, {"ok": False, "error": "account setup failed on gateway"})
            return
        ports = {k: base + rac for k, base in BANDS.items()}
        log.info("REGISTERED site=%s rac=%s user=%s ports=%s (from %s)", site, rac, user, ports, ip)
        kick_failover_sync()
        self._reply(200, {
            "ok": True, "site": site, "rac": rac, "user": user,
            "auto_assigned": auto and existing is None,
            "token": fleet_token(),
            "server_addr": SERVER_ADDR, "server_port": SERVER_PORT,
            "gateways": GATEWAYS,
            "ports": ports,
        })


    def _register_sonde(self):
        """POST /register-sonde {"site": ..., "pubkey": ...} — Wsprsonde
        namespace: random ID from 2000-2099 (sticky per site), ssh port
        35800+ID. Same account/key plumbing as /register."""
        ip = self.client_address[0]
        now = time.monotonic()
        if now - self._last_by_ip.get(ip, -60) < 5:
            self._reply(429, {"ok": False, "error": "slow down"})
            return
        self._last_by_ip[ip] = now
        try:
            length = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(min(length, 65536)))
            site = str(req.get("site", "")).strip().upper()
            pubkey = " ".join(str(req.get("pubkey", "")).split())
        except (ValueError, TypeError):
            self._reply(400, {"ok": False, "error": "bad JSON"})
            return
        if not SITE_RE.match(site):
            self._reply(400, {"ok": False, "error": "bad site name (A-Z 0-9 _ -, 3-32 chars)"})
            return
        if not PUBKEY_RE.match(pubkey):
            self._reply(400, {"ok": False, "error": "pubkey doesn't look like an OpenSSH public key line"})
            return

        # Sticky: a site that's already registered keeps its ID, so
        # re-running the installer is idempotent.
        sonde = None
        if os.path.exists(SONDE_REGISTRY):
            with open(SONDE_REGISTRY) as f:
                for rk, ent in json.load(f).items():
                    if ent["site"] == site:
                        sonde = int(rk)
        if sonde is None:
            sonde = pick_free_sonde()
            if sonde is None:
                log.error("sonde assign: no free ID left in %d-%d", SONDE_MIN, SONDE_MAX)
                self._reply(503, {"ok": False, "error": "no free sonde IDs left — contact the admin"})
                return
            log.info("assigned sonde ID %d to site %s", sonde, site)

        user = key_user(pubkey)
        err = claim(site, sonde, user, registry=SONDE_REGISTRY)
        if err:
            log.warning("SONDE REJECTED %s id=%s site=%s: %s", ip, sonde, site, err)
            self._reply(409, {"ok": False, "error": err})
            return
        try:
            ensure_account(site, pubkey)
        except Exception as e:
            log.error("account setup failed for %s: %s", site, e)
            self._reply(500, {"ok": False, "error": "account setup failed on gateway"})
            return
        ssh_port = 35800 + sonde
        log.info("SONDE REGISTERED site=%s id=%s user=%s ssh_port=%s (from %s)",
                 site, sonde, user, ssh_port, ip)
        kick_failover_sync()
        self._reply(200, {
            "ok": True, "site": site, "sonde": sonde, "user": user,
            "token": fleet_token(),
            "server_addr": SERVER_ADDR, "server_port": SERVER_PORT,
            "gateways": GATEWAYS,
            "ports": {"ssh": ssh_port},
            "admin_keys": sonde_admin_keys(),
        })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=35737)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    fleet_token()  # fail fast if frps config is unreadable
    log.info("rac-registrar v%s listening on :%d", VERSION, args.port)
    ThreadingHTTPServer(("0.0.0.0", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
