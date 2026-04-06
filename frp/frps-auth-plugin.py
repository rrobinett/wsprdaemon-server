#!/usr/bin/env python3
"""
frps-auth-plugin.py — Per-client SSH-key-derived auth for frps v0.64

Design (required by frps v0.64 limitation — httpPlugin cannot override built-in token check):
  - Server and all clients use auth.token = "" (empty) so built-in check always passes
  - Each frpc sets:  user = "<first 16 chars of sha256 of their id_rsa.pub>"
  - This plugin verifies that user field against SHA256 hashes of all keys in
    /home/*/.ssh/authorized_keys — if no match, login is rejected

Client-side token generation (put in frpc.toml):
  user = $(sha256sum ~/.ssh/id_rsa.pub | cut -c1-16)
  auth.token = ""

Revocation: remove the client's public key from authorized_keys.

Usage:
  python3 /home/frp/frps-auth-plugin.py [--port 9001] [--debug]
"""

VERSION = "2.3.0"

import argparse
import glob
import hashlib
import json
import logging
import traceback
from http.server import BaseHTTPRequestHandler, HTTPServer

log = logging.getLogger("frps-auth")


def load_approved_users():
    """
    Scan /home/*/.ssh/authorized_keys, return dict of
    key_prefix (first 16 hex chars of sha256(line+newline)) -> "username:comment"
    """
    users = {}
    for path in glob.glob("/home/*/.ssh/authorized_keys"):
        username = path.split("/")[2]
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    prefix = hashlib.sha256((line + "\n").encode()).hexdigest()[:16]
                    parts = line.split()
                    comment = parts[2] if len(parts) >= 3 else "(no comment)"
                    users[prefix] = f"{username}:{comment}"
        except OSError as e:
            log.warning("Cannot read %s: %s", path, e)
    return users


class AuthHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_POST(self):
        try:
            self._handle_post()
        except Exception:
            log.error("Unhandled exception:\n%s", traceback.format_exc())
            try:
                self.send_response(500)
                self.end_headers()
            except Exception:
                pass

    def _handle_post(self):
        if not self.path.startswith("/handler"):
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        log.debug("raw payload: %s", body.decode(errors="replace"))

        try:
            req = json.loads(body)
        except json.JSONDecodeError:
            log.warning("Invalid JSON: %r", body)
            self._respond(False, "invalid request")
            return

        op = req.get("op", "")
        content = req.get("content", {})
        remote = content.get("client_address", self.client_address[0])

        if op != "Login":
            self._respond(True, "")
            return

        client_user = content.get("user", "")
        log.debug("Login from %s  user=%r", remote, client_user)

        if not client_user:
            log.warning("DENY login from %s -- no user field set in frpc config", remote)
            self._respond(False, "user field required (set user = SHA256[:16] of id_rsa.pub in frpc.toml)")
            return

        approved = load_approved_users()
        identity = approved.get(client_user)

        if identity:
            log.info("ALLOW login from %s -- %s (user=%s)", remote, identity, client_user)
            self._respond(True, "")
        else:
            log.warning("DENY login from %s -- user %r not in authorized_keys", remote, client_user)
            self._respond(False, "unknown user")

    def _respond(self, allow, msg):
        body = json.dumps({"reject": not allow, "reject_reason": msg, "unchange": True}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description=f"frps SSH-key auth plugin v{VERSION}")
    parser.add_argument("--port", type=int, default=9001)
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y/%m/%d %H:%M:%S",
    )

    log.info("frps-auth-plugin v%s starting on 127.0.0.1:%d", VERSION, args.port)
    n = len(load_approved_users())
    log.info("Loaded %d approved key(s) from /home/*/.ssh/authorized_keys", n)

    server = HTTPServer(("127.0.0.1", args.port), AuthHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")


if __name__ == "__main__":
    main()
