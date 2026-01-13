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
        for i, album in enumerate(all_albums):
            if i % 500 == 0:
                print(f"    Progress: {i}/{len(all_albums)}")
                db.commit()  # Commit periodically
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
        
        # Try to get connection, with retry on failure
        max_retries = 2
        roon = None
        for attempt in range(max_retries):
            try:
                roon = get_roon_connection()
                # Test connection is still alive
                roon.browse_browse({"hierarchy": "browse", "pop_all": True})
                break
            except Exception as conn_err:
                print(f"  ⚠ Connection attempt {attempt + 1} failed: {conn_err}")
                # Reset connection and retry
                close_roon_connection()
                if attempt < max_retries - 1:
                    print("  Retrying connection...")
                    time.sleep(2)
                else:
                    raise Exception(f"Could not connect to Roon after {max_retries} attempts")
        
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
            db.update_sync_status('roon_tags', 0, 'failed: Library not found')
            return 0
        
        roon.browse_browse({"hierarchy": "browse", "item_key": library_key})
        time.sleep(0.5)
        load_result = roon.browse_load({"hierarchy": "browse", "offset": 0})
        library_items = load_result.get('items', [])
        
        # Find Tags
        tags_key = next((i.get('item_key') for i in library_items if i.get('title') == 'Tags'), None)
        if not tags_key:
            print("  ✗ Could not find Tags menu")
            db.update_sync_status('roon_tags', 0, 'failed: Tags not found')
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
            db.update_sync_status('roon_tags', 0, 'failed: No target tags')
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
        
        # Ensure columns exist (check first to avoid error)
        db.execute("""
            SELECT COUNT(*) as cnt FROM information_schema.columns 
            WHERE table_schema = DATABASE() 
            AND table_name = 'roon_albums' 
            AND column_name = 'is_physical_dupe'
        """)
        result = db.fetch_one()
        if result['cnt'] == 0:
            db.execute("""
                ALTER TABLE roon_albums 
                ADD COLUMN is_physical_dupe BOOLEAN DEFAULT FALSE,
                ADD COLUMN physical_tag VARCHAR(50) DEFAULT NULL
            """)
            db.commit()
            print("  ✓ Added is_physical_dupe columns")
        
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
        
        db.update_sync_status('roon_tags', matched, 'success')
        return matched
        
    except Exception as e:
        print(f"✗ Roon tags sync failed: {e}")
        import traceback
        traceback.print_exc()
        db.update_sync_status('roon_tags', 0, f'failed: {str(e)[:50]}')
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
        duplicates = []
        
        for i, item in enumerate(all_items):
            if i % 50 == 0:
                print(f"    Progress: {i}/{len(all_items)}")
                db.commit()  # Commit periodically so progress is visible in DB
            
            collection_id, was_duplicate, artist, album_title, release_id = db.insert_discogs_collection(item)
            
            if was_duplicate:
                duplicates.append((artist, album_title, release_id))
            
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
        
        # Report duplicates
        if duplicates:
            print(f"\n  ⚠ Found {len(duplicates)} duplicate(s) - please check library:")
            for artist, album, rid in duplicates:
                print(f"    - {artist} - {album} (release_id: {rid})")
        
        # Sync Last_Listened field (field_id 5) to listening_history
        print("  Syncing Last_Listened data to listening_history...")
        listened_count = 0
        from datetime import datetime
        
        for item in all_items:
            notes = item.get('notes', [])
            last_listened = None
            
            # Find field_id 5 (Last_Listened)
            for note in notes:
                if note.get('field_id') == 5 and note.get('value'):
                    last_listened = note['value']
                    break
            
            if last_listened:
                basic = item.get('basic_information', {})
                artist = basic['artists'][0]['name'] if basic.get('artists') else 'Unknown'
                album_title = basic.get('title', 'Unknown')
                release_id = item.get('id')
                
                # Parse date (format: "Dec 17, 2025")
                try:
                    dt = datetime.strptime(last_listened, "%b %d, %Y")
                    
                    # Update discogs_collection.last_listened
                    db.execute("""
                        UPDATE discogs_collection SET last_listened = %s WHERE release_id = %s
                    """, (dt, release_id))
                    
                    # Check if already in listening_history
                    db.execute("""
                        SELECT id FROM listening_history 
                        WHERE album = %s AND source = 'discogs' AND DATE(listened_at) = DATE(%s)
                    """, (album_title, dt))
                    
                    if not db.fetch_one():
                        db.execute("""
                            INSERT INTO listening_history (artist, album, source, listened_at, notes)
                            VALUES (%s, %s, 'discogs', %s, 'Imported from Discogs Last_Listened field')
                        """, (artist, album_title, dt))
                    
                    listened_count += 1
                except Exception as e:
                    print(f"    Could not parse date '{last_listened}': {e}")
        
        db.commit()
        if listened_count > 0:
            print(f"  ✓ Synced {listened_count} Last_Listened records")
        
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
        for i, item in enumerate(all_items):
            if i % 50 == 0:
                print(f"    Progress: {i}/{len(all_items)}")
                db.commit()  # Commit periodically
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
# TRACK INDEX SYNC
# =============================================================

def sync_tracks_index(db, force=False):
    """
    Build track_index table from roon_tracks and discogs_tracks.
    This creates a denormalized view for fast track browsing.
    """
    print("\n--- Syncing Track Index ---")
    
    try:
        # Create table if not exists
        db.execute("""
            CREATE TABLE IF NOT EXISTS track_index (
                id INT AUTO_INCREMENT PRIMARY KEY,
                track_title VARCHAR(500),
                album VARCHAR(500),
                artist VARCHAR(300),
                source ENUM('roon', 'discogs'),
                INDEX idx_track_title (track_title),
                INDEX idx_artist (artist),
                INDEX idx_album (album)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Truncate and rebuild
        print("  Truncating track_index...")
        db.execute("TRUNCATE TABLE track_index")
        
        # Insert from roon_tracks
        print("  Inserting Roon tracks...")
        db.execute("""
            INSERT INTO track_index (track_title, album, artist, source)
            SELECT track_title, album, album_artist, 'roon'
            FROM roon_tracks
            WHERE track_title IS NOT NULL AND track_title != ''
        """)
        roon_count = db.cursor.rowcount
        db.commit()
        print(f"    ✓ {roon_count:,} Roon tracks")
        
        # Insert from discogs_tracks
        print("  Inserting Discogs tracks...")
        db.execute("""
            INSERT INTO track_index (track_title, album, artist, source)
            SELECT dt.track_title, dc.album_title, dc.artist, 'discogs'
            FROM discogs_tracks dt
            JOIN discogs_collection dc ON dt.collection_id = dc.id
            WHERE dt.track_title IS NOT NULL AND dt.track_title != ''
        """)
        discogs_count = db.cursor.rowcount
        db.commit()
        print(f"    ✓ {discogs_count:,} Discogs tracks")
        
        total = roon_count + discogs_count
        
        # Get distinct count
        db.execute("SELECT COUNT(DISTINCT track_title) as cnt FROM track_index")
        distinct_count = db.fetch_one()['cnt']
        
        print(f"  ✓ Track index built: {total:,} total, {distinct_count:,} distinct titles")
        
        db.update_sync_status('track_index', total, 'success')
        return total
        
    except Exception as e:
        print(f"  ✗ Track index sync failed: {e}")
        db.update_sync_status('track_index', 0, f'failed: {str(e)[:50]}')
        return 0

# =============================================================
# MAIN SYNC FUNCTION
# =============================================================

def sync_all(sources=None, force=False):
    """
    Run full sync of all sources
    
    Args:
        sources: List of source names to sync, or None for all
                 Options: 'roon_albums', 'roon_tags', 'roon_tracks', 'roon_play_history',
                         'discogs_collection', 'discogs_wantlist', 'tracks'
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
        
        # Track Index (depends on roon_tracks and discogs_tracks)
        if sources is None or 'tracks' in sources:
            results['track_index'] = sync_tracks_index(db, force)
        
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
        
        # Save to sync_history (only on full sync)
        if sources is None:
            try:
                # Create table if not exists
                db.execute("""
                    CREATE TABLE IF NOT EXISTS sync_history (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        sync_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        roon_albums INT DEFAULT 0,
                        roon_tracks INT DEFAULT 0,
                        roon_play_history INT DEFAULT 0,
                        discogs_collection INT DEFAULT 0,
                        discogs_tracks INT DEFAULT 0,
                        discogs_wantlist INT DEFAULT 0,
                        track_index_total INT DEFAULT 0,
                        track_index_distinct INT DEFAULT 0,
                        listening_history INT DEFAULT 0,
                        INDEX idx_sync_date (sync_date)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """)
                
                # Get current counts
                db.execute("SELECT COUNT(*) as cnt FROM roon_albums")
                roon_albums = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM roon_tracks")
                roon_tracks = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM roon_play_history")
                roon_play_history = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM discogs_collection")
                discogs_collection = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM discogs_tracks")
                discogs_tracks = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM discogs_wantlist")
                discogs_wantlist = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM track_index")
                track_index_total = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(DISTINCT track_title) as cnt FROM track_index")
                track_index_distinct = db.fetch_one()['cnt']
                
                db.execute("SELECT COUNT(*) as cnt FROM listening_history")
                listening_history = db.fetch_one()['cnt']
                
                # Insert into sync_history
                db.execute("""
                    INSERT INTO sync_history 
                    (roon_albums, roon_tracks, roon_play_history, discogs_collection, 
                     discogs_tracks, discogs_wantlist, track_index_total, 
                     track_index_distinct, listening_history)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (roon_albums, roon_tracks, roon_play_history, discogs_collection,
                      discogs_tracks, discogs_wantlist, track_index_total,
                      track_index_distinct, listening_history))
                db.commit()
                
                print(f"\n✓ Saved to sync_history")
                
            except Exception as hist_err:
                print(f"\n⚠ Could not save to sync_history: {hist_err}")
        
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
                                'discogs_collection', 'discogs_wantlist', 'tracks'],
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
