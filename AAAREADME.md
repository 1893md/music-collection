# Music Collection Sync System

## Overview
Syncs music collection data from Roon and Discogs APIs to a MariaDB database on a Raspberry Pi.

## Directory Structure

```
~/mYdb/
├── Scripts/
│   ├── sync_all.py          # Main sync script
│   ├── db_helper.py         # Database helper class
│   └── .env                  # Environment variables (tokens, credentials)
├── data/
│   ├── LibraryTracks-complete.csv   # Roon tracks export
│   ├── roon_history.json            # Roon play history export
│   └── tagged_albums.json           # Tagged albums export
└── exports/                  # (legacy, not currently used)

~/roon_env/                   # Python virtual environment (alternate)
~/mYdb/roon_env/              # Python virtual environment (primary)
```

## Environment Variables (.env)

```bash
# Database
DB_HOST=192.168.1.14          # Raspberry Pi IP
DB_USER=music_app
DB_PASSWORD=<password>
DB_NAME=music_collection

# Roon
ROON_HOST=172.16.21.225       # Roon Core IP

# Discogs OAuth (required for custom fields like Last_Listened)
DISCOGS_USERNAME=<username>
DISCOGS_KEY=<consumer_key>
DISCOGS_SECRET=<consumer_secret>
DISCOGS_OAUTH_TOKEN=<oauth_token>
DISCOGS_OAUTH_TOKEN_SECRET=<oauth_token_secret>
```

## Token Files

```
~/.roon_token                 # Roon API authorization token
```

## Network Topology

```
Mac (172.16.21.x) ──── MikroTik Switch (172.16.21.1) ──── Netgear Router (192.168.1.1)
                              │                                    │
                       Roon Core                              Raspberry Pi
                    (172.16.21.225)                         (192.168.1.14)
                              │
                          QNAP NAS
              (172.16.21.229 / 192.168.1.3)
```

## sync_all.py Arguments

```bash
# Sync specific source
python sync_all.py --source <source> [--force]
python sync_all.py -s <source> [-f]

# Sync all sources
python sync_all.py [--force]
python sync_all.py --all [-f]
```

### Available Sources

| Source | Description | Type |
|--------|-------------|------|
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
|------|-------------|
| `-f`, `--force` | Bypass 7-day skip check |
| `-s`, `--source` | Sync specific source only |
| `-a`, `--all` | Sync all sources (default) |

## Database Tables

| Table | Description |
|-------|-------------|
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

## Debugging Commands

### Check Database Connection
```bash
mysql -h 192.168.1.14 -u music_app -p music_collection -e "SELECT 1"
```

### Check Table Counts
```sql
SELECT 
  (SELECT COUNT(*) FROM roon_albums) as roon_albums,
  (SELECT COUNT(*) FROM roon_tracks) as roon_tracks,
  (SELECT COUNT(*) FROM discogs_collection) as discogs_collection,
  (SELECT COUNT(*) FROM discogs_tracks) as discogs_tracks;
```

### Check Sync Status
```sql
SELECT * FROM keep_track ORDER BY source_name;
SELECT * FROM sync_history ORDER BY sync_date DESC LIMIT 5;
```

### Check for Lock Issues
```sql
SHOW PROCESSLIST;
SHOW VARIABLES LIKE 'innodb_lock_wait_timeout';
SHOW GLOBAL STATUS LIKE 'Aborted_clients';
```

### Network Debugging
```bash
# Check routes
netstat -rn | grep 192.168

# Test connectivity
ping -c 2 192.168.1.14    # Pi
ping -c 2 172.16.21.225   # Roon

# Test Roon port
nc -zv 172.16.21.225 9330 -w 5
```

### Roon Token Issues
```bash
# View current token
cat ~/.roon_token

# If Roon connection hangs, re-authorize:
mv ~/.roon_token ~/.roon_token.bak
# Then run sync - Roon will prompt for authorization
# Save new token from output
```

### Virtual Environment
```bash
# Activate
source ~/mYdb/roon_env/bin/activate

# Check packages
pip list | grep -E "roonapi|websocket|requests"

# websocket-client version matters - 1.3.3 works
pip show websocket-client
```

### roonapisocket.py Patch
If websocket errors occur, check patch is in place:
```bash
grep "isinstance(message" ~/mYdb/roon_env/lib/python3.14/site-packages/roonapi/roonapisocket.py
```

Should show:
```python
message = message.decode("utf-8") if isinstance(message, bytes) else message if isinstance(message, str) else str(message)
```

## Common Issues

### "Lock wait timeout exceeded"
- Caused by interrupted syncs (Ctrl+C without cleanup)
- Fix: Wait 50 seconds or restart MariaDB
- Prevention: sync_all.py now has signal handler for clean shutdown

### Roon connection hangs
- Usually stale token - re-authorize the extension
- Check Roon Settings → Extensions

### Network routing issues after Mac reboot
- Stale routes can persist - reboot Mac to clear
- Check: `netstat -rn | grep 192.168`

### Discogs rate limiting
- 60 requests/minute limit
- Full collection sync takes 2+ hours
- Run overnight

## Cron Scheduling
```bash
# Edit crontab
crontab -e

# Example: Run full sync on odd days at 7:15 AM
15 7 1-31/2 * * cd ~/mYdb/Scripts && ~/mYdb/roon_env/bin/python sync_all.py --force >> ~/mYdb/logs/sync.log 2>&1
```

## Files Modified During 2026-02-20 Debug Session

1. `~/mYdb/Scripts/sync_all.py` - Added signal handler, null checks, retry logic, fixed file paths
2. `~/mYdb/roon_env/lib/python3.14/site-packages/roonapi/roonapisocket.py` - Patched for websocket-client 1.x compatibility
3. `~/.roon_token` - Re-authorized with new token
