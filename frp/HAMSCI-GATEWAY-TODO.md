# HamSCI RAC gateway: bring it up to the WsprDaemon design

Note recorded 2026-10-24 (Rob). **None of this work has been started.**

> **Update, same day:** the authoritative plan is in the private repo
> `HamSCI/hamsci-remote-admin`, `docs/vpn/redundancy-plan.md`. There, hs0 is
> **vpn1** (`vpn1.hamsci.org`, 23.92.20.151, Newark) and **vpn2 already exists**
> (`vpn2.hamsci.org`, 172.238.180.229, Chicago). It was built 2026-10-01 to a
> hardened baseline only, with frps installed but no configuration. The PI's
> decision is to make vpn2 a hot spare mirroring vpn1 now, failing over by
> moving `vpn.hamsci.org` to it, and to build dual tunnels with Let's Encrypt
> later. Changes to those servers are recorded through PRs to that repo.

## The goal

HamSCI Sigmond clients should get the same remote-access setup that WsprDaemon
clients have today. Today the HamSCI side has one gateway; WsprDaemon has two.

| | WsprDaemon | HamSCI today | HamSCI target |
|---|---|---|---|
| Gateway 1 | gw2 | hs0 (`vpn.hamsci.org`, 10.3.2.1) | hs0, renamed HamSCI VPN1 |
| Gateway 2 | gw1 (standby, state synced from gw2) | vpn2: built to baseline, carries nothing | vpn2 |
| Clients connected to | both gateways at once (`wd-remote-access@gw1` + `@gw2`) | hs0 only | both VPN1 and VPN2 at once |
| Enrollment | TOFU on the open tier, then the secure tier | hand-configured (DASI number gives RAC 220+N) | TOFU on the open tier, then automatic switch to secure |

## Work items

1. **Create HamSCI VPN2.** Provision a second Linode and install the same
   gateway stack as hs0: frps-open, frps-secure, `frps-tofu-plugin`, the
   WireGuard admin network and `rac-dashboard`. Give it a DNS name next to
   `vpn.hamsci.org`.
2. **Make VPN1 and VPN2 mirror each other.** Every client must be accepted on
   either gateway. Replicate the TOFU registry (`/home/frp/rac-tofu-registry.json`)
   and any other gateway state between them, the way gw2 syncs to gw1
   (`/usr/local/sbin/sync-failover-state.sh`). Decide whether the two are
   peers or one is the master.
3. **Clients talk to both gateways all the time.** Each HamSCI Sigmond frpc
   connects to VPN1 and VPN2 at once, like WsprDaemon's `wd-remote-access@gw1`
   + `@gw2`. Then losing one gateway leaves every station still reachable.
   frp has no fallback-server option, so this means two frpc instances per
   station, not a config flag. Client changes go through the Sigmond
   (HamSCI org) repos.
4. **Open-tier TOFU enrollment.** A new HamSCI station first connects on the
   open (unsecured) tier. Its first login there files its key in the TOFU
   registry.
5. **Automatic switchover to the secure tier.** Once the station's key is on
   file, the client moves itself to frps-secure (TLS + TOFU-checked login) and
   stops using the open tier.
6. **Non-DASI stations: supported** (Rob, 2026-10-24). There may be few,
   but they are welcome. Today a HamSCI RAC number comes only from a DASI
   number (RAC 220+N), and there is no registrar on hs0, so non-DASI
   stations need a way to get a RAC number under TOFU.
7. **Dashboard.** VPN2's `rac-dashboard.conf` must list both its open and
   its secure frps, or the stations on the tier it leaves out are invisible.
   hs0 had exactly that bug until 2026-09-16.
8. **Access levels: deferred.** Separating which ports each kind of user may
   reach, with authorized users and permission levels, is a larger problem
   for later. For now anyone who can open the HamSCI dashboard sees
   everything on it: the Proxmox links and the VM links (Rob, 2026-10-24).

## Reference

- WsprDaemon dual-gateway client: `wd-rac-client` (`wd-remote-access@gw1/@gw2`).
- TOFU plugin: `frp/frps-tofu-plugin.py` (v1.0.0, running on hs0).
- Dashboards: `frp/rac-dashboard-server.py` (gw2) and
  `frp/rac-dashboard-server-hamsci.py` (hs0).
