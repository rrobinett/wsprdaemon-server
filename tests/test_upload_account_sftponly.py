"""A station's gateway account must be able to SFTP whichever creates it first.

registrar-sftponly (2026-11-12/15): the RAC registrar and the upload provisioner name the
account alike when a reporter ID has no '/' or '-' (AB4EJ, W7WKR_2, N8UR-SIGMOND after
sanitising).  The registrar made it without the sftponly group and the chroot layout; the
provisioner treated "exists" as done; uploads were refused until someone fixed it by hand
(N8UR-SIGMOND on gw2, 2026-11-14 22:38Z).
"""
from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import types
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import wsprdaemon_server as wds  # noqa: E402


def _provision_script(monkeypatch):
    seen = {}

    def run(argv, input=None, **kw):
        seen["script"] = input.decode()
        return subprocess.CompletedProcess(argv, 0, b"ok\n", b"")
    monkeypatch.setattr(wds.subprocess, "run", run)
    wds._provision_sftp_user_on_gw("gw2.example", "N8UR_SIGMOND", "ssh-ed25519 AAAA test")
    return seen["script"]


def test_provisioner_adds_sftponly_to_an_existing_account(monkeypatch):
    s = _provision_script(monkeypatch)
    # useradd -G only applies on create; an account the registrar made first needs usermod
    assert 'usermod -aG sftponly "$USERNAME"' in s
    assert s.index("usermod -aG sftponly") > s.index("useradd")


def _load_registrar():
    spec = importlib.util.spec_from_file_location("rac_registrar", HERE.parent / "frp" / "rac-registrar.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_registrar_creates_an_account_that_can_sftp(tmp_path, monkeypatch):
    reg = _load_registrar()
    home = tmp_path / "AB4EJ"
    calls = []
    users = {}

    def run(argv, **kw):
        calls.append(argv)
        if argv[0] == "useradd":
            home.mkdir()
            users[argv[-1]] = types.SimpleNamespace(pw_dir=str(home), pw_uid=os.getuid(), pw_gid=os.getgid())
        return subprocess.CompletedProcess(argv, 0, b"", b"")

    def getpwnam(name):
        if name in users:
            return users[name]
        raise KeyError(name)

    chowns = []
    monkeypatch.setattr(reg.subprocess, "run", run)
    monkeypatch.setattr(reg.pwd, "getpwnam", getpwnam)
    monkeypatch.setattr(reg.os, "chown", lambda p, u, g: chowns.append((str(p), u, g)))
    reg.ensure_account("AB4EJ", "ssh-ed25519 AAAAkey rac@AB4EJ")

    useradd = next(c for c in calls if c[0] == "useradd")
    assert "-G" in useradd and useradd[useradd.index("-G") + 1] == "sftponly"
    assert (str(home), 0, 0) in chowns                      # chroot: home owned by root
    assert oct(os.stat(home).st_mode & 0o777) == oct(0o755)
    assert (home / "uploads").is_dir()
    assert (home / ".ssh" / "authorized_keys").read_text().strip() == "ssh-ed25519 AAAAkey rac@AB4EJ"


def test_registrar_leaves_an_existing_account_alone(tmp_path, monkeypatch):
    reg = _load_registrar()
    home = tmp_path / "K3LR_D"
    home.mkdir()
    calls = []
    monkeypatch.setattr(reg.subprocess, "run", lambda argv, **kw: calls.append(argv)
                        or subprocess.CompletedProcess(argv, 0, b"", b""))
    monkeypatch.setattr(reg.pwd, "getpwnam", lambda n: types.SimpleNamespace(
        pw_dir=str(home), pw_uid=os.getuid(), pw_gid=os.getgid()))
    monkeypatch.setattr(reg.os, "chown", lambda *a: None)
    reg.ensure_account("K3LR_D", "ssh-ed25519 AAAAkey2 rac@K3LR")
    assert not any(c[0] in ("useradd", "usermod", "groupadd") for c in calls)
