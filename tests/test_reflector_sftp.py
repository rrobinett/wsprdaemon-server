"""Reflector: delivering to ANOTHER gateway's station-upload account.

Why this transport exists.  A reflector's destinations have always been hosts
we hold a shell on, so `rsync -a --remove-source-files` was all it needed.
Chaining one reflector into another breaks that: it arrives as an ORDINARY
UPLOAD ACCOUNT — the chrooted `sftponly` class every station uses, with
`ForceCommand internal-sftp` and `AllowTcpForwarding no`.  rsync cannot run
there; it needs to exec a remote rsync binary and internal-sftp never gives
it a shell.

Using the station account class rather than inventing a privileged one is the
point: the relay writes into its own chroot and can do nothing else.

Four things must hold, and each one has cost somebody data somewhere:

  * `put -P`, not `put`.  The mtime is load-bearing the whole way down —
    station.heartbeat's `received_at` IS the tar's mtime.  A plain put
    restamps every tar with the time the relay ran, and every station then
    reads as reporting at the relay's cadence instead of its own.
  * `.part` then rename.  The receiving gateway globs `*.tbz` on a 5 s loop
    and would forward a half-written tar under its final name.
  * Delete only what sftp confirmed, only after it did.  A file deleted on a
    failed batch is a station's spots gone, with nothing upstream holding a
    copy.
  * An sftp destination must not be probed for rsync, or be judged on a `df`
    it cannot run.

Run with:  python3 -m pytest tests/test_reflector_sftp.py -v
"""
from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import wsprdaemon_reflector as rf  # noqa: E402


SFTP_DEST = {'name': 'RELAY', 'user': 'relay', 'host': 'relay.example.org',
             'path': 'uploads', 'transport': 'sftp'}
RSYNC_DEST = {'name': 'PEER', 'user': 'wsprdaemon', 'host': 'peer.example.org',
              'path': '/var/spool/wsprdaemon/incoming'}


# ---------- which transport ----------

def test_a_destination_with_no_transport_is_rsync_as_it_always_was():
    """⛔ Every reflector_destinations.json already deployed omits
    this key.  If the default ever moved, both production gateways would
    stop forwarding to the WD servers."""
    assert rf.destination_transport(RSYNC_DEST) == rf.TRANSPORT_RSYNC
    assert rf.destination_transport({'transport': None}) == rf.TRANSPORT_RSYNC
    assert rf.destination_transport({'transport': ' SFTP '}) == rf.TRANSPORT_SFTP


# ---------- the batch ----------

def test_the_batch_preserves_mtime():
    """⛔ `put -P`.  received_at in station.heartbeat IS the tar's mtime —
    how the WD servers learn when a station actually delivered.  A plain
    `put` would restamp every tar with the relay's clock."""
    batch = rf.sftp_batch([Path('/q/AC0G_B4_2610262130.tbz')], 'uploads').decode()
    assert 'put -P /q/AC0G_B4_2610262130.tbz' in batch
    assert 'put /q/' not in batch


def test_the_batch_puts_to_part_then_renames():
    """⛔ The receiving gateway globs *.tbz on a 5 s loop.  A tar still being
    written under its final name is one it can hard-link and forward half
    complete."""
    batch = rf.sftp_batch([Path('/q/x.tbz')], 'uploads').decode()
    lines = batch.strip().splitlines()
    assert lines == ['put -P /q/x.tbz uploads/x.tbz.part',
                     'rename uploads/x.tbz.part uploads/x.tbz']


def test_every_file_gets_its_own_put_and_rename():
    batch = rf.sftp_batch([Path('/q/a.tbz'), Path('/q/b.tbz')], 'up').decode()
    assert batch.count('put -P') == 2
    assert batch.count('rename') == 2
    assert batch.strip().splitlines()[-1] == 'rename up/b.tbz.part up/b.tbz'


# ---------- verification and space ----------

def test_an_sftp_destination_is_never_probed_for_rsync():
    """⛔ `ssh <sftponly account> which rsync` returns 'This service allows
    sftp connections only' and a non-zero rc, which would DISABLE a
    destination that works perfectly."""
    with mock.patch.object(rf, 'verify_destination_rsync') as probe:
        valid = rf.verify_all_destinations({'destinations': [SFTP_DEST]})
    probe.assert_not_called()
    assert valid == [SFTP_DEST]


def test_an_rsync_destination_is_still_probed():
    """Mutation guard: skipping the probe for everything would hide a
    destination that genuinely lacks rsync."""
    with mock.patch.object(rf, 'verify_destination_rsync',
                           return_value=True) as probe:
        rf.verify_all_destinations({'destinations': [RSYNC_DEST]})
    probe.assert_called_once()


def test_space_on_an_sftp_destination_reads_full_not_unknown():
    """⛔ `df` needs a shell a chrooted account does not have.  Returning
    None makes the caller treat it as a TIMED-OUT check and skip the sync —
    so a healthy destination would never receive a file while the log blamed
    disk space."""
    with mock.patch.object(rf.subprocess, 'run') as run:
        free = rf.check_remote_free_space(SFTP_DEST, 'uploads')
    run.assert_not_called()
    assert free == 100.0


# ---------- rrsync-confined destinations ----------

CONFINED_DEST = {'name': 'TARGET', 'user': 'relay', 'host': 'target.example.org',
                 'path': '.', 'confined': True, 'ssh_key': '/etc/example/id_ed25519'}


def test_a_confined_destination_still_uses_rsync():
    """rrsync IS rsync, with a fixed root and write-only enforcement.  The
    transport does not change; only our ability to run anything else does."""
    assert rf.destination_transport(CONFINED_DEST) == rf.TRANSPORT_RSYNC
    assert rf.destination_has_shell(CONFINED_DEST) is False


def test_an_ordinary_rsync_destination_still_has_a_shell():
    """⛔ Mutation guard.  An ordinary destination has a full shell and both
    checks must keep running there — losing them would hide a destination
    that genuinely lacks rsync or is full."""
    assert rf.destination_has_shell(RSYNC_DEST) is True


def test_a_confined_destination_is_not_probed_for_rsync():
    """⛔ `ssh HOST 'which rsync'` against an authorized_keys pinned to
    `command="rrsync -wo ~/uploads"` runs RRSYNC, not which, and fails — so
    the probe would DISABLE a destination that works perfectly."""
    with mock.patch.object(rf, 'verify_destination_rsync') as probe:
        valid = rf.verify_all_destinations({'destinations': [CONFINED_DEST]})
    probe.assert_not_called()
    assert valid == [CONFINED_DEST]


def test_space_on_a_confined_destination_reads_full_not_unknown():
    with mock.patch.object(rf.subprocess, 'run') as run:
        free = rf.check_remote_free_space(CONFINED_DEST, 'uploads')
    run.assert_not_called()
    assert free == 100.0


# ---------- the ssh options (port is load-bearing) ----------

def test_a_non_default_port_reaches_every_transport():
    """⛔ A destination outside its own network is not necessarily reachable
    on 22.  Without the port every delivery dials 22 and times out, and a
    timeout reads as "the destination is down" rather than "we called the
    wrong number"."""
    opts = rf.destination_ssh_opts({'port': 2022, 'ssh_key': '/k'})
    assert '-p' in opts and '2022' in opts
    assert '-i' in opts and '/k' in opts


def test_no_port_means_no_p_flag_so_ssh_uses_its_default():
    """Mutation guard: existing deployed configs declare no port and must
    keep reaching their destinations on ssh's default."""
    opts = rf.destination_ssh_opts({'ssh_key': '/k'})
    assert '-p' not in opts


def test_the_port_reaches_the_sftp_argv_too():
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp, names=('a.tbz',))
        dest = {**SFTP_DEST, 'port': 2222}
        with mock.patch.object(rf.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, b'', b'')
            _Worker(q, dest=dest)._sync_over_sftp('/key')
        cmd = run.call_args.args[0]
        assert '-p' in cmd and '2222' in cmd


# ---------- delivery ----------

class _Worker:
    """The parts of the sync worker _sync_over_sftp touches."""
    def __init__(self, queue_dir, dest=None):
        self.queue_dir = queue_dir
        self.destination = dest or SFTP_DEST
        self.config = {'rsync_timeout': 300, 'rsync_bandwidth_limit': 20000}
    _sync_over_sftp = rf.RsyncWorker._sync_over_sftp


def _queue(tmp, names=('a.tbz', 'b.tbz')):
    q = Path(tmp)
    for n in names:
        (q / n).write_bytes(b'tar')
    return q


def test_a_successful_batch_removes_the_queue_copies():
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp)
        with mock.patch.object(rf.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, b'', b'')
            _Worker(q)._sync_over_sftp('/key')
        assert sorted(p.name for p in q.iterdir()) == []


def test_a_FAILED_batch_keeps_every_file_for_the_next_pass():
    """⛔ The safety of the whole queue.  sftp -b stops at the first failing
    command and exits non-zero, so a partial batch must delete NOTHING — a
    file dropped here is a station's spots gone, and nothing upstream still
    holds a copy.  Re-delivering a duplicate is harmless: the gateway
    hard-links by name and the server de-duplicates by hash."""
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp)
        with mock.patch.object(rf.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess(
                [], 1, b'', b'remote open failed: Permission denied')
            _Worker(q)._sync_over_sftp('/key')
        assert sorted(p.name for p in q.iterdir()) == ['a.tbz', 'b.tbz']


def test_a_TIMEOUT_keeps_every_file():
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp)
        with mock.patch.object(rf.subprocess, 'run',
                               side_effect=subprocess.TimeoutExpired([], 1)):
            _Worker(q)._sync_over_sftp('/key')
        assert len(list(q.iterdir())) == 2


def test_an_empty_queue_opens_no_connection():
    with tempfile.TemporaryDirectory() as tmp:
        with mock.patch.object(rf.subprocess, 'run') as run:
            _Worker(Path(tmp))._sync_over_sftp('/key')
        run.assert_not_called()


def test_dotfiles_in_the_queue_are_not_shipped():
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp, names=('a.tbz',))
        (q / '.partial').write_bytes(b'x')
        with mock.patch.object(rf.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, b'', b'')
            _Worker(q)._sync_over_sftp('/key')
        batch = run.call_args.kwargs['input'].decode()
        assert '.partial' not in batch
        assert (q / '.partial').exists()


def test_the_sftp_argv_names_the_account_the_key_and_a_per_host_socket():
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp, names=('a.tbz',))
        with mock.patch.object(rf.subprocess, 'run') as run:
            run.return_value = subprocess.CompletedProcess([], 0, b'', b'')
            _Worker(q)._sync_over_sftp('/home/wsprdaemon/.ssh/id_rsa')
        cmd = run.call_args.args[0]
        assert cmd[:3] == ['sftp', '-b', '-']
        assert cmd[-1] == 'relay@relay.example.org'
        assert '/home/wsprdaemon/.ssh/id_rsa' in cmd
        path = [v for f, v in zip(cmd, cmd[1:]) if v.startswith('ControlPath=')]
        assert path and '%h' in path[0], \
            "the socket must vary per host, or one host's master serves another"


def test_an_rsync_destination_never_takes_the_sftp_path():
    """Mutation guard: the branch must be selected by the transport key."""
    with tempfile.TemporaryDirectory() as tmp:
        q = _queue(tmp, names=('a.tbz',))
        w = _Worker(q, dest=RSYNC_DEST)
        assert rf.destination_transport(w.destination) == rf.TRANSPORT_RSYNC


if __name__ == '__main__':
    import pytest
    sys.exit(pytest.main([__file__, '-v']))
