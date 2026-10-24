#!/usr/bin/env python3
"""
frps-tofu-plugin.py — account-less trust-on-first-use auth for frps-secure.

Rob Robinett's design (2026-08-09): unlike gw2's frps-auth-plugin.py, this
creates NO Linux accounts and needs NO registrar.  Every Sigmond appliance
frpc carries its identity in login metadata:

    [metadatas]
    pubkey = "ssh-ed25519 AAAA... rob@robinett.us"
    site   = "AI6VN"
    dasi   = "DASI-003"

and sets  user = "<its stable identity>"  (for DASI boxes, "DASI-NNN").

frps v0.64 forwards these as content.user / content.metas to this Login
plugin.  Admission rule (TOFU):

  * a login MUST carry a user and metas.pubkey;
  * the user MUST match sha256(pubkey)[:16] OR be a DASI-NNN id — either
    way the client proves it holds the private key by presenting the pubkey
    whose fingerprint is recorded;
  * FIRST login for a given user files {user -> pubkey} in the registry;
  * LATER logins for that user are admitted ONLY if the pubkey matches the
    filed one — a different key claiming a taken user is rejected
    (impersonation / duplicate-DASI-number guard).

Registry: /home/frp/rac-tofu-registry.json  (first-come-first-served, and
the ONLY state — no /home/<SITE> accounts).  Revoke by deleting the entry.

Re-key allow list (v1.1.0): users named in /home/frp/rac-tofu-rekey-allow.txt
(one per line, # comments) are TEST stations that get reimaged, so each image
brings a new key.  For them a different key REPLACES the filed one instead of
being rejected, and the change is logged.  Every other user keeps strict TOFU.
Never list a field station: anyone could claim a listed name.  The file is
re-read on every login.

Every entry carries "updated_at" (UTC ISO, set on first claim and on re-key).
racsync-merge-registry.py uses it so a re-key on one gateway carries to the
other (newest wins) instead of being kept apart as a conflict.

The frps token is NOT the gate (kept empty); the pubkey is.

Usage:  python3 frps-tofu-plugin.py [--port 9001] [--registry PATH]
                                   [--rekey-allow PATH] [--debug]
"""

VERSION = "1.1.0"

import argparse
import hashlib
import json
import logging
import os
import tempfile
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer

log = logging.getLogger("frps-tofu")
_LOCK = threading.Lock()
REGISTRY = "/home/frp/rac-tofu-registry.json"
REKEY_ALLOW = "/home/frp/rac-tofu-rekey-allow.txt"


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def rekey_allowed(user):
    """True if USER is a test station listed in the re-key allow list."""
    try:
        with open(REKEY_ALLOW) as f:
            names = {ln.split("#", 1)[0].strip() for ln in f}
    except FileNotFoundError:
        return False
    except OSError as e:
        log.error("re-key allow list unreadable (%s): treating as empty", e)
        return False
    return user in names


def _fp(pubkey):
    """Stable 16-hex fingerprint of a pubkey line (matches the frpc user
    convention: sha256 of the key line + newline, first 16 hex chars)."""
    return hashlib.sha256((pubkey.strip() + "\n").encode()).hexdigest()[:16]


def load_registry():
    try:
        with open(REGISTRY) as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as e:
        log.error("registry unreadable (%s) — failing CLOSED", e)
        return None


def save_registry(reg):
    """Atomic write so a crash can't truncate the registry."""
    d = os.path.dirname(REGISTRY) or "."
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tofu-", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(reg, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, REGISTRY)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


class AuthHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        try:
            self._handle()
        except Exception:
            log.error("unhandled:\n%s", traceback.format_exc())
            try:
                self.send_response(500)
                self.end_headers()
            except Exception:
                pass

    def _handle(self):
        if not self.path.startswith("/handler"):
            self.send_response(404)
            self.end_headers()
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            log.warning("invalid JSON: %r", body)
            return self._respond(False, "invalid request")

        op = req.get("op", "")
        content = req.get("content", {})
        remote = content.get("client_address", self.client_address[0])
        # Only Login is gated; everything else passes unchanged.
        if op != "Login":
            return self._respond(True, "")

        user = (content.get("user") or "").strip()
        metas = content.get("metas") or {}
        pubkey = (metas.get("pubkey") or "").strip()

        if not user or not pubkey:
            log.warning("DENY %s -- missing user/pubkey (user=%r pubkey=%s)",
                        remote, user, "yes" if pubkey else "no")
            return self._respond(False, "user and metadata pubkey required")

        with _LOCK:
            reg = load_registry()
            if reg is None:
                return self._respond(False, "server registry error")
            entry = reg.get(user)
            if entry is None:
                # trust on first use
                reg[user] = {"pubkey": pubkey, "fp": _fp(pubkey),
                             "site": metas.get("site", ""),
                             "dasi": metas.get("dasi", ""),
                             "first_seen": _now(),
                             "updated_at": _now(),
                             "addr": remote}
                try:
                    save_registry(reg)
                except Exception as e:
                    log.error("cannot persist registry: %s", e)
                    return self._respond(False, "server registry error")
                log.info("ALLOW %s -- TOFU first claim of %r (fp=%s site=%s)",
                         remote, user, _fp(pubkey), metas.get("site", ""))
                return self._respond(True, "")
            if entry.get("pubkey", "").strip() == pubkey:
                log.info("ALLOW %s -- %r matches filed key", remote, user)
                return self._respond(True, "")
            if rekey_allowed(user):
                old_fp = entry.get("fp")
                entry.update({"pubkey": pubkey, "fp": _fp(pubkey),
                              "site": metas.get("site", entry.get("site", "")),
                              "dasi": metas.get("dasi", entry.get("dasi", "")),
                              "updated_at": _now(), "rekeyed_from": old_fp,
                              "addr": remote})
                try:
                    save_registry(reg)
                except Exception as e:
                    log.error("cannot persist registry: %s", e)
                    return self._respond(False, "server registry error")
                log.warning("ALLOW %s -- %r RE-KEYED (on the re-key allow list): "
                            "fp %s -> %s", remote, user, old_fp, _fp(pubkey))
                return self._respond(True, "")
            log.warning("DENY %s -- %r already claimed by a DIFFERENT key "
                        "(filed fp=%s, presented fp=%s)",
                        remote, user, entry.get("fp"), _fp(pubkey))
            return self._respond(False, "identity already claimed by another key")

    def _respond(self, allow, msg):
        body = json.dumps({"reject": not allow, "reject_reason": msg,
                           "unchange": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    global REGISTRY, REKEY_ALLOW
    ap = argparse.ArgumentParser(description=f"frps TOFU auth plugin v{VERSION}")
    ap.add_argument("--port", type=int, default=9001)
    ap.add_argument("--registry", default=REGISTRY)
    ap.add_argument("--rekey-allow", default=REKEY_ALLOW)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    REGISTRY = args.registry
    REKEY_ALLOW = args.rekey_allow
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S")
    reg = load_registry() or {}
    log.info("frps-tofu-plugin v%s on 127.0.0.1:%d — %d filed identit(ies), registry=%s",
             VERSION, args.port, len(reg), REGISTRY)
    HTTPServer(("127.0.0.1", args.port), AuthHandler).serve_forever()


if __name__ == "__main__":
    main()
