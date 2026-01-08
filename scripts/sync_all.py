#!/usr/bin/env python3
"""
sync_all.py - Unified sync script for Music Collection
Handles both API sources and file sources
Checks timestamps and only syncs when needed
"""

import os
import json
import csv
import time
from datetime import datetime, timedelta
from db_helper import MusicDB

# =============================================================
# CONFIGURATION
# =============================================================

ROON_HOST = os.getenv('ROON_HOST', 'your_roon_core_ip')  # Update with your Roon Core IP
ROON_PORT = 9330
ROON_TOKEN_FILE = os.path.expanduser("~/.roon_token")

DISCOGS_TOKEN = os.getenv('DISCOGS_TOKEN', 'YOUR_DISCOGS_TOKEN_HERE')
DISCOGS_USERNAME = os.getenv('DISCOGS_USERNAME', 'your_username_here')
DISCOGS_HEADERS = {
    'Authorization': f'Discogs token={DISCOGS_TOKEN}',
    'User-Agent': 'MusicCollectionManager/1.0'
}

# Skip API syncs if synced within this many days
SKIP_DAYS = 7

# =============================================================
# HELPER FUNCTIONS
# =============================================================

def should_skip_sync(db, source_name, force=False):
    """Check if we should skip syncing based on last sync time"""
    if force:
        return False
    
    last_sync, _ = db.get_last_sync(source_name)
    if not last_sync:
        return False
    
    days_since = (datetime.now() - last_sync).days
    if days_since < SKIP_DAYS:
        print(f"  ⏭ Skipping {source_name} - synced {days_since} days ago (use --force to override)")
        return True
    
    return False

# =============================================================
# ROON API FUNCTIONS
# =============================================================

_roon_connection = None

def get_roon_connection():
    """Establish connection to Roon (reuses existing connection)"""
    global _roon_connection
    
    if _roon_connection is not None:
        return _roon_connection
    
    from roonapi import RoonApi
    
    appinfo = {
        "extension_id": "my_roon_extension",
        "display_name": "Music Collection Sync",
        "display_version": "1.0.0",
        "publisher": "Michael",
        "email": "user@example.com"
    }
    
    with open(ROON_TOKEN_FILE, 'r') as f:
        saved_token = f.read().strip()
    
    _roon_connection = RoonApi(appinfo, saved_token, ROON_HOST, ROON_PORT)
    return _roon_connection

def close_roon_connection():
    """Close the Roon connection"""
    global _roon_connection
    _roon_connection = None

def sync_roon_albums(db, force=False):
    """Sync albums from Roon API to database"""
    print("\n" + "="*60)
    print("SYNCING ROON ALBUMS (API)")
    print("="*60)
    
    if should_skip_sync(db, 'roon_albums', force):
        return db.get_table_count('roon_albums')
    
    try:
        global _roon_connection
        already_connected = _roon_connection is not None
        
        roon = get_roon_connection()
        if not already_connected:
            print(f"✓ Connected to Roon: {roon.core_name}")
            time.sleep(2)
        else:
            print(f"  Using existing Roon connection")
        
        # Navigate: Root -> Library -> Albums
        print("  Navigating to Albums...")
        roon.browse_browse({"hierarchy": "browse", "pop_all": True})
        time.sleep(0.5)
        load_result = roon.browse_load({"hierarchy": "browse", "offset": 0})
        root_items = load_result.get('items', [])
        
        # Find Library
        library_key = next((i.get('item_key') for i in root_items if i.get('title') == 'Library'), None)
        if not library_key:
            print("  ✗ Could not find Library menu")
            db.update_sync_status('roon_albums', 0, 'failed: Library not found')
            return 0
        
        roon.browse_browse({"hierarchy": "browse", "item_key": library_key})
        time.sleep(0.5)
        load_result = roon.browse_load({"hierarchy": "browse", "offset": 0})
        library_items = load_result.get('items', [])
        
        # Find Albums
        albums_key = next((i.get('item_key') for i in library_items if i.get('title') == 'Albums'), None)
        if not albums_key:
            print("  ✗ Could not find Albums menu")
            db.update_sync_status('roon_albums', 0, 'failed: Albums not found')
            return 0
        
        browse_result = roon.browse_browse({"hierarchy": "browse", "item_key": albums_key})
        time.sleep(0.5)
        
        album_count = browse_result.get('list', {}).get('count', 0) if browse_result else 0
        print(f"  Found {album_count:,} albums in Roon")
        
        if album_count == 0:
            print("  ✗ No albums found")
            db.update_sync_status('roon_albums', 0, 'failed: No albums found')
            return 0
        
        # Truncate and reload
        db.truncate_table('roon_albums')
        
        # Load all albums
        all_albums = []
        offset = 0
        
        while offset < album_count:
            load_opts = {
                "hierarchy": "browse",
                "offset": offset,
            }
            load_result = roon.browse_load(load_opts)
            items = load_result.get('items', [])
            if not items:
                break
            all_albums.extend(items)
            offset += len(items)
            
            if offset % 500 == 0 or offset >= album_count:
                print(f"    Loaded {offset:,} of {album_count:,} albums...")
            
            time.sleep(0.1)
        
        # Insert into database
        print(f"  Inserting {len(all_albums):,} albums into database...")
        for album in all_albums:
            artist = album.get('subtitle', 'Unknown')
            title = album.get('title', 'Unknown')
            
            db.insert_roon_album({
                'artist': artist,
                'title': title,
                'image_key': album.get('image_key'),
                'item_key': album.get('item_key')
            })
        
        db.commit()
        
        # Update keep_track
        db.update_sync_status('roon_albums', len(all_albums), 'success')
        
        print(f"✓ Synced {len(all_albums):,} Roon albums")
        return len(all_albums)
        
    except Exception as e:
        print(f"✗ Roon sync failed: {e}")
        import traceback
        traceback.print_exc()
        db.update_sync_status('roon_albums', 0, f'failed: {str(e)[:50]}')
        return 0


def sync_roon_tags(db, force=False):
    """Fetch albums tagged myCDs/mYLps from Roon and flag as physical duplicates"""
    print("\n" + "="*60)
    print("SYNCING ROON TAGS (Physical Duplicates)")
    print("="*60)
    
    try:
        global _roon_connection
        already_connected = _roon_connection is not None
        
        roon = get_roon_connection()
        if not already_connected:
            print(f"✓ Connected to Roon: {roon.core_name}")
            time.sleep(2)
        else:
            print(f"  Using existing Roon connection")
        
        # Navigate: Root -> Library -> Tags
        print("  Navigating to Tags...")
        roon.browse_browse({"hierarchy": "browse", "pop_all": True})
        time.sleep(0.5)
        load_result = roon.browse_load({"hierarchy": "browse", "offset": 0})
        root_items = load_result.get('items', [])
        
        # Find Library
        library_key = next((i.get('item_key') for i in root_items if i.get('title') == 'Library'), None)
        if not library_key:
            print("  ✗ Could not find Library menu")
            return 0
        
        roon.browse_browse({"hierarchy": "browse", "item_key": library_key})
        time.sleep(0.5)
        load_result = roon.browse_load({"hierarchy": "browse", "offset": 0})
        library_items = load_result.get('items', [])
        
        # Find Tags
        tags_key = next((i.get('item_key') for i in library_items if i.get('title') == 'Tags'), None)
        if not tags_key:
            print("  ✗ Could not find Tags menu")
            return 0
        
        roon.browse_browse({"hierarchy": "browse", "item_key": tags_key})
        time.sleep(0.5)
        load_result = roon.browse_load({"hierarchy": "browse", "offset": 0})
        tag_items = load_result.get('items', [])
        
        print(f"  Found {len(tag_items)} tags")
        
        # Find myCDs and mYLps (case-insensitive)
        target_tags = {}
        for tag in tag_items:
            title = tag.get('title', '')
            if title.lower() in ['mycds', 'mylps']:
                target_tags[title] = tag.get('item_key')
        
        if not target_tags:
            print("  ✗ Could not find myCDs or mYLps tags")
            return 0
        
        print(f"  Found target tags: {list(target_tags.keys())}")
        
        # Fetch all albums for each tag
        tagged_albums = []
        
        for tag_name, tag_key in target_tags.items():
            print(f"  Fetching albums for '{tag_name}'...")
            
            result = roon.browse_browse({"hierarchy": "browse", "item_key": tag_key})
            time.sleep(0.5)
            
            total = result.get('list', {}).get('count', 0)
            print(f"    Total: {total} albums")
            
            offset = 0
            while offset < total:
                load_result = roon.browse_load({"hierarchy": "browse", "offset": offset})
                items = load_result.get('items', [])
                
                for item in items:
                    title = item.get('title', '')
                    
                    # Skip "Play Tag" action item
                    if title == 'Play Tag':
                        continue
                    
                    tagged_albums.append({
                        'album_title': title,
                        'tag': tag_name
                    })
                
                offset += len(items)
                if not items:
                    break
                
                time.sleep(0.1)
        
        print(f"  Total tagged albums: {len(tagged_albums)}")
        
        # Ensure columns exist
        try:
            db.execute("""
                ALTER TABLE roon_albums 
                ADD COLUMN is_physical_dupe BOOLEAN DEFAULT FALSE,
                ADD COLUMN physical_tag VARCHAR(50) DEFAULT NULL
            """)
            db.commit()
            print("  ✓ Added is_physical_dupe columns")
        except:
            pass  # Columns already exist
        
        # Reset all flags
        db.execute("UPDATE roon_albums SET is_physical_dupe = FALSE, physical_tag = NULL")
        db.commit()
        
        # Update albums that match
        matched = 0
        for album in tagged_albums:
            db.execute("""
                UPDATE roon_albums 
                SET is_physical_dupe = TRUE, physical_tag = %s
                WHERE LOWER(album_title) = LOWER(%s)
            """, (album['tag'], album['album_title']))
            matched += db.cursor.rowcount
        
        db.commit()
        
        print(f"✓ Flagged {matched} albums as physical duplicates")
        
        # Show breakdown
        db.execute("""
            SELECT physical_tag, COUNT(*) as cnt 
            FROM roon_albums 
            WHERE is_physical_dupe = TRUE 
            GROUP BY physical_tag
        """)
        for row in db.fetch_all():
            print(f"    {row['physical_tag']}: {row['cnt']} albums")
        
        return matched
        
    except Exception as e:
        print(f"✗ Roon tags sync failed: {e}")
        import traceback
        traceback.print_exc()
        return 0


# =============================================================
# DISCOGS API FUNCTIONS
# =============================================================

def sync_discogs_collection(db, force=False):
    """Sync collection from Discogs API to database"""
    import requests
    
    print("\n" + "="*60)
    print("SYNCING DISCOGS COLLECTION (API)")
    print("="*60)
    
    if should_skip_sync(db, 'discogs_collection', force):
        return db.get_table_count('discogs_collection')
    
    # Get token at runtime (not module load time)
    discogs_token = os.getenv('DISCOGS_TOKEN', DISCOGS_TOKEN)
    discogs_headers = {
        'Authorization': f'Discogs token={discogs_token}',
        'User-Agent': 'MusicCollectionManager/1.0'
    }
    
    try:
        # Fetch collection
        all_items = []
        page = 1
        per_page = 100
        
        while True:
            url = f'https://api.discogs.com/users/{DISCOGS_USERNAME}/collection/folders/0/releases'
            params = {'page': page, 'per_page': per_page}
            
            response = requests.get(url, headers=discogs_headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                items = data['releases']
                all_items.extend(items)
                
                print(f"  Fetched page {page}/{data['pagination']['pages']} ({len(all_items):,} items)")
                
                if page >= data['pagination']['pages']:
                    break
                
                page += 1
                time.sleep(1)
            elif response.status_code == 429:
                print("  Rate limited, waiting 10 seconds...")
                time.sleep(10)
            else:
                print(f"✗ API error: {response.status_code}")
                break
        
        print(f"  Total items fetched: {len(all_items):,}")
        
        # Get marketplace values for each item
        print("  Fetching marketplace values...")
        for i, item in enumerate(all_items):
            if i % 50 == 0:
                print(f"    Progress: {i}/{len(all_items)}")
            
            release_id = item['id']
            stats_url = f'https://api.discogs.com/marketplace/stats/{release_id}'
            
            try:
                stats_response = requests.get(stats_url, headers=discogs_headers)
                if stats_response.status_code == 200:
                    item['marketplace_stats'] = stats_response.json()
                elif stats_response.status_code == 429:
                    time.sleep(10)
                    stats_response = requests.get(stats_url, headers=discogs_headers)
                    if stats_response.status_code == 200:
                        item['marketplace_stats'] = stats_response.json()
            except:
                pass
            
            time.sleep(2)  # Rate limiting
        
        # Truncate and reload
        db.truncate_table('discogs_tracks')
        db.truncate_table('discogs_collection')
        
        # Insert into database
        print(f"  Inserting {len(all_items):,} items into database...")
        track_count = 0
        
        for item in all_items:
            collection_id = db.insert_discogs_collection(item)
            
            # Get full release details for tracklist
            release_id = item['id']
            release_url = f'https://api.discogs.com/releases/{release_id}'
            
            try:
                release_response = requests.get(release_url, headers=discogs_headers)
                if release_response.status_code == 200:
                    release_data = release_response.json()
                    tracklist = release_data.get('tracklist', [])
                    
                    for track in tracklist:
                        db.insert_discogs_track(collection_id, release_id, track)
                        track_count += 1
                elif release_response.status_code == 429:
                    time.sleep(10)
            except:
                pass
            
            time.sleep(2)  # Rate limiting
        
        db.commit()
        
        # Update keep_track
        db.update_sync_status('discogs_collection', len(all_items), 'success')
        db.update_sync_status('discogs_tracks', track_count, 'success')
        
        print(f"✓ Synced {len(all_items):,} Discogs collection items")
        print(f"✓ Synced {track_count:,} Discogs tracks")
        return len(all_items)
        
    except Exception as e:
        print(f"✗ Discogs collection sync failed: {e}")
        db.update_sync_status('discogs_collection', 0, f'failed: {str(e)[:50]}')
        return 0

def sync_discogs_wantlist(db, force=False):
    """Sync wantlist from Discogs API to database"""
    import requests
    
    print("\n" + "="*60)
    print("SYNCING DISCOGS WANTLIST (API)")
    print("="*60)
    
    if should_skip_sync(db, 'discogs_wantlist', force):
        return db.get_table_count('discogs_wantlist')
    
    # Get token at runtime (not module load time)
    discogs_token = os.getenv('DISCOGS_TOKEN', DISCOGS_TOKEN)
    discogs_headers = {
        'Authorization': f'Discogs token={discogs_token}',
        'User-Agent': 'MusicCollectionManager/1.0'
    }
    
    try:
        # Fetch wantlist
        all_items = []
        page = 1
        per_page = 100
        
        while True:
            url = f'https://api.discogs.com/users/{DISCOGS_USERNAME}/wants'
            params = {'page': page, 'per_page': per_page}
            
            response = requests.get(url, headers=discogs_headers, params=params)
            
            if response.status_code == 200:
                data = response.json()
                items = data['wants']
                all_items.extend(items)
                
                print(f"  Fetched page {page}/{data['pagination']['pages']} ({len(all_items):,} items)")
                
                if page >= data['pagination']['pages']:
                    break
                
                page += 1
                time.sleep(1)
            elif response.status_code == 429:
                print("  Rate limited, waiting 10 seconds...")
                time.sleep(10)
            else:
                print(f"✗ API error: {response.status_code}")
                break
        
        print(f"  Total wantlist items: {len(all_items):,}")
        
        # Get marketplace values
        print("  Fetching marketplace values...")
        for i, item in enumerate(all_items):
            if i % 50 == 0:
                print(f"    Progress: {i}/{len(all_items)}")
            
            release_id = item['id']
            stats_url = f'https://api.discogs.com/marketplace/stats/{release_id}'
            
            try:
                stats_response = requests.get(stats_url, headers=discogs_headers)
                if stats_response.status_code == 200:
                    item['marketplace_stats'] = stats_response.json()
                elif stats_response.status_code == 429:
                    time.sleep(10)
            except:
                pass
            
            time.sleep(2)
        
        # Truncate and reload
        db.truncate_table('discogs_wantlist')
        
        # Insert into database
        print(f"  Inserting {len(all_items):,} wantlist items...")
        for item in all_items:
            db.insert_discogs_wantlist(item)
        
        db.commit()
        
        # Update keep_track
        db.update_sync_status('discogs_wantlist', len(all_items), 'success')
        
        print(f"✓ Synced {len(all_items):,} wantlist items")
        return len(all_items)
        
    except Exception as e:
        print(f"✗ Discogs wantlist sync failed: {e}")
        db.update_sync_status('discogs_wantlist', 0, f'failed: {str(e)[:50]}')
        return 0

# =============================================================
# FILE IMPORT FUNCTIONS
# =============================================================

def get_file_modified_time(file_path):
    """Get file modification timestamp"""
    try:
        mtime = os.path.getmtime(file_path)
        return datetime.fromtimestamp(mtime)
    except:
        return None

def sync_roon_tracks(db, force=False):
    """Sync Roon tracks from CSV file if newer than last sync"""
    print("\n" + "="*60)
    print("SYNCING ROON TRACKS (FILE)")
    print("="*60)
    
    last_sync, file_path = db.get_last_sync('roon_tracks')
    
    if not file_path:
        print("✗ No file path configured for roon_tracks")
        return 0
    
    if not os.path.exists(file_path):
        print(f"✗ File not found: {file_path}")
        return 0
    
    file_modified = get_file_modified_time(file_path)
    print(f"  File: {file_path}")
    print(f"  File modified: {file_modified}")
    print(f"  Last sync: {last_sync}")
    
    if not force and file_modified and last_sync and file_modified <= last_sync:
        print("  ⏭ File not modified since last sync - skipping")
        return db.get_table_count('roon_tracks')
    
    # File is newer - import it
    print("  File is newer - importing...")
    
    # Truncate table
    db.truncate_table('roon_tracks')
    
    # Read and insert CSV
    record_count = 0
    
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            db.insert_roon_track(row)
            record_count += 1
            
            if record_count % 10000 == 0:
                print(f"    Imported {record_count:,} tracks...")
                db.commit()
    
    db.commit()
    
    # Update keep_track
    db.update_sync_status('roon_tracks', record_count, 'success')
    
    print(f"✓ Imported {record_count:,} Roon tracks")
    return record_count

def sync_roon_play_history(db, force=False):
    """Sync Roon play history from JSON file if newer than last sync"""
    print("\n" + "="*60)
    print("SYNCING ROON PLAY HISTORY (FILE)")
    print("="*60)
    
    last_sync, file_path = db.get_last_sync('roon_play_history')
    
    if not file_path:
        print("✗ No file path configured for roon_play_history")
        return 0
    
    if not os.path.exists(file_path):
        print(f"✗ File not found: {file_path}")
        return 0
    
    file_modified = get_file_modified_time(file_path)
    print(f"  File: {file_path}")
    print(f"  File modified: {file_modified}")
    print(f"  Last sync: {last_sync}")
    
    if not force and file_modified and last_sync and file_modified <= last_sync:
        print("  ⏭ File not modified since last sync - skipping")
        return db.get_table_count('roon_play_history')
    
    # File is newer - import it
    print("  File is newer - importing...")
    
    # Truncate table
    db.truncate_table('roon_play_history')
    
    # Read and insert JSON
    with open(file_path, 'r', encoding='utf-8') as f:
        history_data = json.load(f)
    
    record_count = 0
    for record in history_data:
        db.insert_roon_play(record)
        record_count += 1
        
        if record_count % 5000 == 0:
            print(f"    Imported {record_count:,} plays...")
            db.commit()
    
    db.commit()
    
    # Update keep_track
    db.update_sync_status('roon_play_history', record_count, 'success')
    
    print(f"✓ Imported {record_count:,} Roon play history records")
    return record_count

# =============================================================
# MAIN SYNC FUNCTION
# =============================================================

def sync_all(sources=None, force=False):
    """
    Run full sync of all sources
    
    Args:
        sources: List of source names to sync, or None for all
                 Options: 'roon_albums', 'roon_tags', 'roon_tracks', 'roon_play_history',
                         'discogs_collection', 'discogs_wantlist'
        force: If True, ignore skip logic and sync anyway
    """
    print("\n" + "="*60)
    print("MUSIC COLLECTION SYNC")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    db = MusicDB()
    if not db.connect():
        print("✗ Failed to connect to database")
        return
    
    results = {}
    
    try:
        # API Sources
        if sources is None or 'roon_albums' in sources:
            results['roon_albums'] = sync_roon_albums(db, force)
        
        # Sync Roon tags (physical duplicates) - runs after roon_albums or standalone
        if sources is None or 'roon_albums' in sources or 'roon_tags' in sources:
            results['roon_tags'] = sync_roon_tags(db, force)
        
        if sources is None or 'discogs_collection' in sources:
            results['discogs_collection'] = sync_discogs_collection(db, force)
        
        if sources is None or 'discogs_wantlist' in sources:
            results['discogs_wantlist'] = sync_discogs_wantlist(db, force)
        
        # File Sources
        if sources is None or 'roon_tracks' in sources:
            results['roon_tracks'] = sync_roon_tracks(db, force)
        
        if sources is None or 'roon_play_history' in sources:
            results['roon_play_history'] = sync_roon_play_history(db, force)
        
        # Print summary
        print("\n" + "="*60)
        print("SYNC COMPLETE")
        print("="*60)
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("\nResults:")
        for source, count in results.items():
            print(f"  {source}: {count:,} records")
        
        # Show keep_track status
        print("\nCurrent keep_track status:")
        db.execute("SELECT source_name, last_sync, records_count, sync_status FROM keep_track ORDER BY source_name")
        for row in db.fetch_all():
            print(f"  {row['source_name']}: {row['records_count'] or 0:,} records @ {row['last_sync']} ({row['sync_status']})")
        
    except Exception as e:
        print(f"\n✗ Sync failed with error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        close_roon_connection()
        db.disconnect()

# =============================================================
# COMMAND LINE INTERFACE
# =============================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Sync Music Collection Data')
    parser.add_argument('--source', '-s', 
                        choices=['roon_albums', 'roon_tags', 'roon_tracks', 'roon_play_history',
                                'discogs_collection', 'discogs_wantlist'],
                        help='Sync specific source only')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Sync all sources (default)')
    parser.add_argument('--force', '-f', action='store_true',
                        help='Force sync even if recently synced')
    
    args = parser.parse_args()
    
    if args.source:
        sync_all(sources=[args.source], force=args.force)
    else:
        sync_all(force=args.force)
