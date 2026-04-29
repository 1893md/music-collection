# Music Collection Sync System

*Last updated: 2026-04-28*

## Overview
Syncs music collection data from Roon and Discogs APIs into a MariaDB database on a Raspberry Pi. The Pi also hosts three Flask APIs (Music Collection, Health Tracker, Energy Dashboard) reverse-proxied through Apache and exposed via a Cloudflare tunnel at https://1893md.net.

Code backed up at https://github.com/1893md/music-collection

---

## Quick Recovery: "sync_all.py won't connect"

If you came here because sync just failed with a connection error, work this checklist before anything else. **The most common cause is the Pi's IP changing** — the rest of this README has the details.

1. **Can the Mac reach the Pi at all?**
   ```
   ping -c 2 apachepi
   ```
   - Replies → Pi reachable, skip to step 3.
   - Timeouts → step 2.

2. **The Pi may have moved.** Find it:
   ```
   arp -a | grep '192.168.1'
   nmap -sn 192.168.1.0/24    # if nmap installed
   ```
   Or look at the Pi directly (HDMI + keyboard) and run `ip a`. Note the new IP.
   - Update `/etc/hosts` on the Mac so `apachepi` points at the new IP.
   - If the Pi has drifted again, the static config has come undone — see *Pi Static IP Setup*.

3. **Is MariaDB actually listening?**
   ```
   nc -vz apachepi 3306
   ```
   - "succeeded" → DB is up; problem is elsewhere (auth, schema, lock — see *Common Issues*).
   - "Connection refused" → MariaDB isn't running. SSH in and check (`sudo systemctl status mariadb`).

4. **Is the Mac's network sane?**
   ```
   netstat -rn | grep default
   ifconfig en0 | grep 'inet '
   ```
   Default gateway should be `172.16.21.1`. Mac's IP should be on `172.16.21.x`. If you're on a corporate VPN, multiple `utun*` interfaces in `ifconfig | grep utun` will hijack routing — disconnect the VPN.

5. **Last resort:** `sudo killall -HUP mDNSResponder` on the Mac to flush DNS cache, or reboot the Mac. macOS sometimes installs reject routes that block 192.168.1.x reachability until reboot.

---

## Network Topology

Two physical networks, joined by the QNAP NAS having one NIC on each. **They do NOT bridge** — each NIC is just a host on its own subnet. The Mac on the 172 side reaches the 192 side by going **back out through the deJitter Switch's WAN port to the main router**, which then delivers to the 192 subnet. Two-hop traceroute, no special routing needed on the Mac.

```
                    Internet
                       |
              Netgear Nighthawk R8000
               (192.168.1.1, DHCP, default gateway)
                       |
        +--------------+------------------+
        |                                 |
  192.168.1.x (main network)              |
        |                                 |
   +---------+                            |
   |  Pi     |  <-- apachepi              |
   |  .21    |      MariaDB, Apache,      |
   |         |      Flask x3, cloudflared |
   +---------+                            |
        |                                 |
   +---------+                            |
   | dnspi   |  .53  (custom dnsmasq)     |
   +---------+                            |
        |                                 |
   +---------+                            |
   |  QNAP   |  NIC #2: 192.168.1.3       |
   |   NAS   |                            |
   |         |  NIC #1: 172.16.21.229  <--+   (separate cable, NOT bridged)
   +----+----+
        |
  172.16.21.x (clean audio network)
        |
   +----------+
   | deJitter |  172.16.21.1
   | Switch X |  (NATs to its WAN port -> Netgear)
   | MikroTik |  Own DHCP, own SSID
   | CRS309   |
   +----+-----+
        |
   +-------------------------------+
   | Roon Core (ROC)  172.16.21.225|
   | Mac (sometimes)  172.16.21.215|
   | Audio gear                    |
   +-------------------------------+
```

### Key host IPs

| Host | IP | Network | Notes |
|---|---|---|---|
| Netgear router | 192.168.1.1 | 192.168.1.x | Default gateway; aging hardware |
| Raspberry Pi (`apachepi`) | 192.168.1.21 | 192.168.1.x | Static via netplan. MariaDB, Apache, 3 Flask apps, cloudflared. |
| QNAP NAS | 192.168.1.3 / 172.16.21.229 | both | Dual NIC, NOT bridging |
| dnspi | 192.168.1.53 | 192.168.1.x | Custom dnsmasq with blocklists. Mnemonic: `.53` = DNS port. |
| deJitter Switch X | 172.16.21.1 | 172.16.21.x | MikroTik CRS309 underneath. NATs to main router. |
| Roon Core (ROC) | 172.16.21.225 | 172.16.21.x | Music server |
| Mac | 172.16.21.215 (typical) | 172.16.21.x | DHCP from deJitter |

### Pi history (for context if it happens again)
The Pi's IP has drifted: was `.14`, then `.10`, now `.21`. Each shift happened silently because the Netgear's DHCP pool covers nearly the whole subnet (`.2–.254` by default) and the Pi previously had no static config. **As of 2026-04-28, the Pi is statically pinned to `.21` via a NetworkManager connection profile** at `/etc/NetworkManager/system-connections/eth0-static.nmconnection` — see *Pi Static IP Setup*.

If `.21` ever doesn't work and the Pi has somehow drifted again, find it via `arp -a` from the Mac or `ip a` on the Pi console.

### Why `192.168.1.14` was wrong before

The old `.env` had `DB_HOST=192.168.1.14`. That worked back when the Pi happened to be at `.14`. After the Pi drifted to `.10` and then `.21`, `.14` started pointing at *some other host* that was up (responded to ping) but had nothing on port 3306 — hence "Connection refused" at the MariaDB layer. **Lesson: use a hostname (`apachepi`) in `.env`, not a hardcoded IP.** A single `/etc/hosts` edit then fixes everything if the Pi ever moves again.

---

## Pi Web Stack

The Pi runs a fleet of services. Apache serves as the front door on port 80; cloudflared tunnels it out to https://1893md.net.

```
Internet -> Cloudflare tunnel -> cloudflared (Pi)
              -> Apache :80
                   -> /         -> static files (in ~/music-collection/html)
                   -> /api      -> flask-music     :5001
                   -> /health   -> health-tracker  :5002
                   -> /energy/  -> energy-dashboard:5050
```

### Service / port map

| Port | Listener | systemd service | Source path |
|---|---|---|---|
| 22 | sshd | ssh | (system) |
| 80 | apache2 | apache2 | `/etc/apache2/sites-enabled/*.conf` |
| 3306 | mariadbd | mariadb | (system) |
| 5001 | python (Flask) | flask-music | `/home/deph/music-collection/scripts/app.py` |
| 5002 | python (Flask) | health-tracker | `/home/deph/health-tracker/health_app.py` |
| 5050 | python (Flask) | energy-dashboard | `/home/deph/electric-dashboard/api/app.py` |
| 20241 | cloudflared | cloudflared | (tunnel local mgmt) |
| 111 | rpcbind | (system) | NFS portmapper, ambient |
| 631 | cupsd | cups | localhost only, fine |

### Apache reverse proxy rules

Defined in `/etc/apache2/sites-enabled/*.conf`:

```apache
<VirtualHost *:80>
    ProxyPass        /energy/  http://localhost:5050/
    ProxyPassReverse /energy/  http://localhost:5050/
    ProxyPass        /api      http://localhost:5001/api
    ProxyPassReverse /api      http://localhost:5001/api
    ProxyPass        /health   http://localhost:5002
    ProxyPassReverse /health   http://localhost:5002
</VirtualHost>
```

Note the small inconsistency: `/energy/` has trailing slashes; `/api` and `/health` don't. Functional, but worth knowing if URLs ever look odd.

### Two paths into Flask
The Flask apps are bound to `0.0.0.0`, so the LAN can hit them directly at e.g. `http://192.168.1.21:5001`, bypassing both Apache and Cloudflare. This is useful for local debugging but means firewall changes need to consider both paths.

---

## Directory Structure

### On the Mac (where sync_all runs)
```
~/mYdb/
+-- Scripts/
|   +-- sync_all.py          # Main sync script
|   +-- db_helper.py         # Database helper class
|   +-- .env                 # Environment variables (tokens, credentials)
+-- data/
|   +-- LibraryTracks-complete.csv
|   +-- roon_history.json
|   +-- tagged_albums.json
+-- exports/                 # (legacy, not currently used)

~/mYdb/roon_env/             # Python virtual environment (primary)
~/roon_env/                  # Python virtual environment (alternate)
```

### On the Pi
```
/home/deph/
+-- music-collection/
|   +-- scripts/             # app.py (port 5001), sync_all.py, db_helper.py, etc.
|   +-- html/                # static files served by Apache at /
|   +-- images/  jscripts/   # static assets
|   +-- venv/
+-- electric-dashboard/
|   +-- api/                 # app.py (port 5050), config.py, import_csv.py
|   +-- index.html  energy-dashboard.conf  Usage*.csv
|   +-- venv/
+-- health-tracker/
|   +-- health_app.py        # port 5002
|   +-- venv/
```

---

## Environment Variables (.env on the Mac)

```
# Database
DB_HOST=apachepi             # Resolves via /etc/hosts to 192.168.1.21
DB_USER=music_app
DB_PASSWORD=<password>
DB_NAME=music_collection

# Roon
ROON_HOST=172.16.21.225      # Roon Core IP
ROON_PORT=9330

# Discogs OAuth (required for custom fields like Last_Listened)
DISCOGS_USERNAME=<username>
DISCOGS_KEY=<consumer_key>
DISCOGS_SECRET=<consumer_secret>
DISCOGS_OAUTH_TOKEN=<oauth_token>
DISCOGS_OAUTH_TOKEN_SECRET=<oauth_token_secret>
```

### Mac /etc/hosts entry

```
192.168.1.21  apachepi
```

If the Pi ever moves IPs, edit this one line and `.env` doesn't need to change.

## Token Files

```
~/.roon_token                # Roon API authorization token
```

---

## Pi Static IP Setup (Trixie / NetworkManager — pure nmcli)

The Pi runs Debian 13 (Trixie). Despite Trixie shipping with both netplan and NetworkManager, **the netplan path is unreliable on this hardware**: edits don't propagate cleanly, NM generates competing volatile profiles in `/run/NetworkManager/system-connections/` that win the boot race, and `netplan try` rewrites the YAML on revert. We learned this the hard way on 2026-04-28.

**Source of truth for eth0 IP config:**
```
/etc/NetworkManager/system-connections/eth0-static.nmconnection
```

The netplan YAML at `/etc/netplan/90-NM-<uuid>.yaml` is annotated to direct readers here and should not be edited.

### Current config

```ini
[connection]
id=eth0-static
type=ethernet
autoconnect-priority=100
interface-name=eth0

[ethernet]

[ipv4]
address1=192.168.1.21/24
dns=192.168.1.53;192.168.1.1;
gateway=192.168.1.1
method=manual

[ipv6]
addr-gen-mode=default
method=auto
```

The `autoconnect-priority=100` matters — it ensures this profile wins over any volatile auto-generated DHCP profile NM may create at boot.

### To change the IP, gateway, or DNS

```bash
# Modify in place
sudo nmcli connection modify eth0-static ipv4.addresses 192.168.1.NEW/24
sudo nmcli connection modify eth0-static ipv4.gateway 192.168.1.1
sudo nmcli connection modify eth0-static ipv4.dns "192.168.1.53,192.168.1.1"

# Apply
sudo nmcli connection down eth0 ; sudo nmcli connection up eth0-static
```

### To recreate from scratch (if the file is ever lost)

```bash
sudo nmcli connection add \
  type ethernet \
  con-name eth0-static \
  ifname eth0 \
  ipv4.method manual \
  ipv4.addresses 192.168.1.21/24 \
  ipv4.gateway 192.168.1.1 \
  ipv4.dns "192.168.1.53,192.168.1.1" \
  connection.autoconnect yes \
  connection.autoconnect-priority 100

sudo nmcli connection down eth0 ; sudo nmcli connection up eth0-static
```

### Verify the static config is healthy

```bash
ip a show eth0
```
Expect `inet 192.168.1.21/24 ... noprefixroute eth0` and `valid_lft forever`. If you see `dynamic` or a numeric `valid_lft`, the static config isn't active — DHCP is.

```bash
nmcli -t -f NAME,DEVICE,STATE connection show --active
```
Expect `eth0-static:eth0:activated`. If you see `eth0:eth0:activated` instead, a volatile auto-generated DHCP profile has taken over (this happened pre-fix and shouldn't recur, but worth checking).

```bash
nmcli device show eth0 | grep -E 'IP4.ADDRESS|IP4.GATEWAY|IP4.DNS'
```
Expect IP `.21`, gateway `.1`, DNS `.53` then `.1`.

### Why netplan doesn't work here (postmortem)

1. Editing `/etc/netplan/*.yaml` and running `sudo netplan apply` *does* regenerate `/run/NetworkManager/system-connections/netplan-eth0.nmconnection`, but NM may already have the interface managed by a separate volatile profile (`eth0.nmconnection`, marked `external=true`, generated at early boot). The netplan-generated profile then loses on autoconnect.
2. `sudo netplan try` followed by a timeout-revert *rewrites* the on-disk YAML back to its pre-edit state — destroying your edit silently.
3. The `routes:` syntax in netplan YAML doesn't always translate to `ipv4.gateway` in the resulting NM profile.
4. NM connection files generated from netplan land in `/run/NetworkManager/system-connections/` (volatile, wiped each boot), not `/etc/...`.

A persistent NM profile in `/etc/NetworkManager/system-connections/` with high autoconnect-priority sidesteps all four problems.

---
---

## sync_all.py Arguments

```
# Sync specific source
python sync_all.py --source <source> [--force]
python sync_all.py -s <source> [-f]

# Sync all sources
python sync_all.py [--force]
python sync_all.py --all [-f]
```

### Available sources

| Source | Description | Type |
|---|---|---|
| `roon_albums` | Albums from Roon API | API |
| `roon_tags` | Physical duplicate tags (myCDs, mYLps) | API |
| `roon_tracks` | Tracks from LibraryTracks-complete.csv | File |
| `roon_play_history` | Play history from roon_history.json | File |
| `discogs_collection` | Collection + tracks from Discogs API | API |
| `discogs_wantlist` | Wantlist from Discogs API | API |
| `listening_history` | Format backfill from roon_albums tags | DB |
| `tracks` | Rebuild track_index from source tables | DB |

### Flags

| Flag | Description |
|---|---|
| `-f`, `--force` | Bypass 7-day skip check |
| `-s`, `--source` | Sync specific source only |
| `-a`, `--all` | Sync all sources (default) |

## Database Tables

| Table | Description |
|---|---|
| `roon_albums` | Albums from Roon library |
| `roon_tracks` | Tracks from Roon library |
| `roon_play_history` | Play history from Roon |
| `discogs_collection` | Physical collection from Discogs |
| `discogs_tracks` | Tracks for Discogs releases |
| `discogs_wantlist` | Wantlist from Discogs |
| `listening_history` | Combined listening history |
| `track_index` | Unified track index (roon + discogs) |
| `keep_track` | Sync status for each source |
| `sync_history` | Historical sync counts |

---

## Debugging Commands

### Check database connection (from Mac)
```
mysql -h apachepi -u music_app -p music_collection -e "SELECT 1"
```

### Check table counts
```sql
SELECT 
  (SELECT COUNT(*) FROM roon_albums) as roon_albums,
  (SELECT COUNT(*) FROM roon_tracks) as roon_tracks,
  (SELECT COUNT(*) FROM discogs_collection) as discogs_collection,
  (SELECT COUNT(*) FROM discogs_tracks) as discogs_tracks;
```

### Check sync status
```sql
SELECT * FROM keep_track ORDER BY source_name;
SELECT * FROM sync_history ORDER BY sync_date DESC LIMIT 5;
```

### Check for lock issues
```sql
SHOW PROCESSLIST;
SHOW VARIABLES LIKE 'innodb_lock_wait_timeout';
SHOW GLOBAL STATUS LIKE 'Aborted_clients';
```

### Network debugging (from Mac)
```
# Mac's routing
netstat -rn | grep default
ifconfig en0 | grep 'inet '

# Anything bridged through utun? (corporate VPN)
ifconfig | grep utun

# Which 192.168.1.x hosts has the Mac talked to recently?
arp -a | grep 192.168.1

# Trace the path to the Pi (should be 2 hops via 172.16.21.1)
traceroute -n -m 5 apachepi

# Test ports on the Pi
nc -vz apachepi 22         # SSH
nc -vz apachepi 3306       # MariaDB
nc -vz apachepi 80         # Apache
nc -vz apachepi 5001       # flask-music (direct)
nc -vz apachepi 5002       # health-tracker (direct)
nc -vz apachepi 5050       # energy-dashboard (direct)
```

### Pi service health (run on the Pi via SSH)
```
sudo systemctl status mariadb
sudo systemctl status apache2
sudo systemctl status cloudflared
sudo systemctl status flask-music
sudo systemctl status health-tracker
sudo systemctl status energy-dashboard
sudo ss -tlnp                    # all listening ports + processes
ip a                             # Pi's actual IP, gateway, DNS
```

### Roon token issues
```
# View current token
cat ~/.roon_token

# If Roon connection hangs, re-authorize:
mv ~/.roon_token ~/.roon_token.bak
# Then run sync — Roon will prompt for authorization
# Save the new token from output
```

### Virtual environment
```
source ~/mYdb/roon_env/bin/activate
pip list | grep -E "roonapi|websocket|requests"
pip show websocket-client          # 1.3.3 known to work
```

### roonapisocket.py patch
If websocket errors occur, verify the patch is in place:
```
grep "isinstance(message" ~/mYdb/roon_env/lib/python3.14/site-packages/roonapi/roonapisocket.py
```
Should show:
```python
message = message.decode("utf-8") if isinstance(message, bytes) else message if isinstance(message, str) else str(message)
```

---

## Common Issues

### "Lock wait timeout exceeded"
- Caused by interrupted syncs (Ctrl+C without cleanup, network drop mid-sync, MariaDB restart mid-sync).
- Fix: Wait ~50 seconds for lock timeout, or restart MariaDB on the Pi.
- Prevention: `sync_all.py` has signal handlers for clean shutdown — but a yanked TCP connection can still leave a session open server-side until timeout.

### Roon connection hangs
- Usually a stale token. Re-authorize via Roon Settings → Extensions, or rotate `~/.roon_token`.

### "Connection refused" on port 3306
- Almost always: wrong `DB_HOST` in `.env`, or the Pi has moved IPs.
- Use the *Quick Recovery* checklist at the top.
- Note: a `Connection refused` (RST) means *some* host responded — it's not a routing problem, it's that nothing is listening at that IP:port. If `nc -vz` to the same IP on port 22 also refuses, something is wrong on the Pi. If 22 succeeds but 3306 refuses, MariaDB is the only thing down.

### Discogs rate limiting
- 60 requests/minute limit.
- Full collection sync takes 2+ hours.
- Run overnight, or use `--force` selectively per source.

### Pi IP drift (the recurring problem)
- Symptom: sync stops working out of nowhere; "Connection refused" on what was a known-good IP.
- Cause: Pi got a new DHCP lease different from what `.env` / `/etc/hosts` expects.
- Fix: now prevented by static IP on the Pi. If it happens again, see *Pi Static IP Setup*.

### macOS reject routes / corporate VPN interference
- Symptom: Mac can't reach Pi even though network looks fine.
- Cause: macOS sometimes installs reject routes (`!` flag in `netstat -rn`); corporate VPN with multiple `utun*` interfaces hijacks the default route.
- Fix: disconnect VPN if active. If reject routes persist after VPN disconnect, reboot the Mac. (`sudo route delete -net 192.168.1.0/24` may also work but is fiddly.)

### "1893md.net is up but I can't reach the Pi locally"
- Cloudflare tunnel runs from cloudflared on the Pi to Cloudflare's edge, independent of LAN reachability. So `1893md.net` returning HTTP 200 doesn't prove the Pi is reachable from your LAN — it only proves cloudflared and Apache are up on the Pi. Always test LAN reachability with `ping`/`nc` directly to `apachepi`, not by checking the public site.

---

## Cron Scheduling
```
crontab -e

# Run full sync on odd days at 7:15 AM
15 7 1-31/2 * * cd ~/mYdb/Scripts && ~/mYdb/roon_env/bin/python sync_all.py --force >> ~/mYdb/logs/sync.log 2>&1
```

---

## History of debug sessions

### 2026-04-28
- Pi found at `192.168.1.21` (had been `.14` historically, then `.10`, now `.21`); root cause: no static IP on Pi.
- `.env` had stale `DB_HOST=192.168.1.14` — replaced with hostname `apachepi`.
- `/etc/hosts` on Mac now maps `apachepi -> 192.168.1.21`.
- DNS for the Pi pointed at `dnspi` (`192.168.1.53`) with router fallback.
- Confirmed topology: deJitter routes between 172 and 192 networks via NAT through main router; QNAP is dual-NIC but NOT bridging.
- Confirmed: MariaDB runs on the Pi, not the QNAP. Old notes claiming otherwise were wrong.
- Mapped full Pi web stack: Apache reverse-proxies `/api` -> 5001, `/health` -> 5002, `/energy/` -> 5050.
- **Static IP attempt 1 (netplan) failed.** Edited `/etc/netplan/90-NM-<uuid>.yaml` to set `dhcp4: false` with addresses/routes/nameservers. After `netplan apply`, `ip a` still showed `dynamic`/`valid_lft` and DNS only had the router. After reboot, the YAML had been overwritten back to `dhcp4: true` (the earlier `netplan try` revert had silently rewritten it). Multiple competing NM profiles in `/run/NetworkManager/system-connections/` (`eth0.nmconnection` marked `external=true`, plus `netplan-eth0.nmconnection`) were racing.
- **Static IP attempt 2 (pure nmcli) worked.** Created `/etc/NetworkManager/system-connections/eth0-static.nmconnection` with `nmcli connection add ... autoconnect-priority 100`. Activated, verified `valid_lft forever` and DNS `.53,.1`, then rebooted. Survived reboot cleanly. The volatile `eth0.nmconnection` no longer regenerates because eth0 is owned by `eth0-static` from boot.
- Annotated the netplan YAML with a comment block directing future readers to the NM profile.

### 2026-02-20
- Patched `roonapisocket.py` for websocket-client 1.x compatibility.
- Added signal handler to `sync_all.py` for clean shutdown.
- Re-authorized Roon token.
