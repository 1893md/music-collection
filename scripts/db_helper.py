"""
db_helper.py - Database connection and helper functions
Music Collection Management System
"""

import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv
from datetime import datetime
import re

# Load environment variables
load_dotenv()

class MusicDB:
    """Database helper class for music collection management"""
    
    def __init__(self):
        self.config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', 'music_app'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'music_collection'),
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci'
        }
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor(dictionary=True)
            print(f"✓ Connected to MySQL database: {self.config['database']}")
            return True
        except Error as e:
            print(f"✗ Database connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            try:
                self.cursor.fetchall()  # Consume any unread results
            except:
                pass
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("✓ Database connection closed")
    
    def execute(self, query, params=None):
        """Execute a query"""
        try:
            self.cursor.execute(query, params or ())
            return True
        except Error as e:
            print(f"✗ Query failed: {e}")
            print(f"  Query: {query[:100]}...")
            return False
    
    def execute_many(self, query, data):
        """Execute a query with multiple rows"""
        try:
            self.cursor.executemany(query, data)
            return True
        except Error as e:
            print(f"✗ Batch insert failed: {e}")
            return False
    
    def fetch_all(self):
        """Fetch all results"""
        return self.cursor.fetchall()
    
    def fetch_one(self):
        """Fetch single result"""
        return self.cursor.fetchone()
    
    def commit(self):
        """Commit transaction"""
        self.conn.commit()
    
    def rollback(self):
        """Rollback transaction"""
        self.conn.rollback()
    
    # =========================================================
    # KEEP_TRACK METHODS
    # =========================================================
    
    def get_last_sync(self, source_name):
        """Get last sync timestamp for a source"""
        self.execute(
            "SELECT last_sync, file_path FROM keep_track WHERE source_name = %s",
            (source_name,)
        )
        result = self.fetch_one()
        if result:
            return result['last_sync'], result['file_path']
        return None, None
    
    def update_sync_status(self, source_name, records_count, status='success'):
        """Update keep_track after sync"""
        self.execute("""
            UPDATE keep_track 
            SET last_sync = NOW(), 
                records_count = %s, 
                sync_status = %s,
                updated_at = NOW()
            WHERE source_name = %s
        """, (records_count, status, source_name))
        self.commit()
        print(f"  ✓ Updated keep_track: {source_name} = {records_count} records ({status})")
    
    # =========================================================
    # TABLE MANAGEMENT
    # =========================================================
    
    def truncate_table(self, table_name):
        """Empty a table (faster than DELETE)"""
        # Disable foreign key checks temporarily
        self.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.execute(f"TRUNCATE TABLE {table_name}")
        self.execute("SET FOREIGN_KEY_CHECKS = 1")
        self.commit()
        print(f"  ✓ Truncated table: {table_name}")
    
    def get_table_count(self, table_name):
        """Get row count for a table"""
        self.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
        result = self.fetch_one()
        return result['cnt'] if result else 0
    
    # =========================================================
    # NORMALIZATION HELPERS
    # =========================================================
    
    @staticmethod
    def normalize_string(s):
        """Normalize string for matching - remove special chars, lowercase, strip 'the'"""
        if not s or s is None:
            return ''
        # Convert to string and lowercase
        s = str(s).lower()
        # Remove special characters except spaces
        s = re.sub(r'[^a-z0-9\s]', '', s)
        # Remove extra whitespace
        s = ' '.join(s.split())
        # Remove leading 'the '
        if s.startswith('the '):
            s = s[4:]
        return s
    
    @staticmethod
    def create_match_key(artist, album):
        """Create normalized match key from artist and album"""
        artist_norm = MusicDB.normalize_string(artist)
        album_norm = MusicDB.normalize_string(album)
        return f"{artist_norm} - {album_norm}"
    
    # =========================================================
    # ROON METHODS
    # =========================================================
    
    def insert_roon_album(self, album_data):
        """Insert or update a Roon album"""
        artist = album_data.get('artist', 'Unknown')
        album_title = album_data.get('title', album_data.get('album', 'Unknown'))
        
        self.execute("""
            INSERT INTO roon_albums 
                (album_title, artist, image_key, item_key, artist_norm, album_norm, match_key)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                album_title = VALUES(album_title),
                artist = VALUES(artist),
                image_key = VALUES(image_key),
                updated_at = NOW()
        """, (
            album_title[:500],
            artist[:300],
            album_data.get('image_key', '')[:100] if album_data.get('image_key') else None,
            album_data.get('item_key', '')[:50] if album_data.get('item_key') else None,
            self.normalize_string(artist)[:300],
            self.normalize_string(album_title)[:500],
            self.create_match_key(artist, album_title)[:500]
        ))
    
    def insert_roon_track(self, track_data):
        """Insert a Roon track"""
        self.execute("""
            INSERT INTO roon_tracks 
                (album_artist, album, disc_number, track_number, track_title, 
                 track_artists, composers, external_id, source, is_duplicate, is_hidden, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(track_data.get('Album Artist', ''))[:300],
            str(track_data.get('Album', ''))[:500],
            track_data.get('Disc#'),
            track_data.get('Track#'),
            str(track_data.get('Title', ''))[:500],
            str(track_data.get('Track Artist(s)', ''))[:500] if track_data.get('Track Artist(s)') else None,
            str(track_data.get('Composer(s)', ''))[:500] if track_data.get('Composer(s)') else None,
            str(track_data.get('External Id', ''))[:100] if track_data.get('External Id') else None,
            str(track_data.get('Source', ''))[:50],
            track_data.get('Is Dup?', 'no').lower() == 'yes',
            track_data.get('Is Hidden?', 'no').lower() == 'yes',
            str(track_data.get('Tags', '')) if track_data.get('Tags') else None
        ))
    
    def insert_roon_play(self, play_data):
        """Insert a Roon play history record"""
        self.execute("""
            INSERT INTO roon_play_history 
                (album_artist, album, disc_number, track_number, track_title,
                 track_artists, composers, external_id, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            str(play_data.get('Album Artist', ''))[:300],
            str(play_data.get('Album', ''))[:500],
            play_data.get('Disc#'),
            play_data.get('Track#'),
            str(play_data.get('Title', ''))[:500],
            str(play_data.get('Track Artist(s)', ''))[:500] if play_data.get('Track Artist(s)') else None,
            str(play_data.get('Composer(s)', ''))[:500] if play_data.get('Composer(s)') else None,
            str(play_data.get('External Id', ''))[:100] if play_data.get('External Id') else None,
            str(play_data.get('Source', ''))[:50]
        ))
    
    # =========================================================
    # DISCOGS METHODS
    # =========================================================
    
    def insert_discogs_collection(self, item):
        """Insert or update a Discogs collection item"""
        basic = item.get('basic_information', {})
        artist = basic['artists'][0]['name'] if basic.get('artists') else 'Unknown'
        album_title = basic.get('title', 'Unknown')
        
        # Extract condition from notes
        notes = item.get('notes', [])
        media_condition = ''
        sleeve_condition = ''
        if isinstance(notes, list):
            for note in notes:
                if note.get('field_id') == 1:
                    media_condition = note.get('value', '')
                elif note.get('field_id') == 2:
                    sleeve_condition = note.get('value', '')
        
        # Get marketplace stats
        stats = item.get('marketplace_stats', {})
        lowest_price = None
        if stats and stats.get('lowest_price'):
            lowest_price = stats['lowest_price'].get('value')
        
        self.execute("""
            INSERT INTO discogs_collection 
                (release_id, instance_id, artist, album_title, label, format, year,
                 date_added, rating, folder_id, artist_norm, album_norm, match_key,
                 num_for_sale, lowest_price, thumb_url, cover_image_url,
                 media_condition, sleeve_condition)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                artist = VALUES(artist),
                album_title = VALUES(album_title),
                num_for_sale = VALUES(num_for_sale),
                lowest_price = VALUES(lowest_price),
                updated_at = NOW()
        """, (
            item.get('id'),
            item.get('instance_id'),
            artist[:300],
            album_title[:500],
            basic['labels'][0]['name'][:300] if basic.get('labels') else None,
            basic['formats'][0]['name'][:100] if basic.get('formats') else None,
            basic.get('year'),
            item.get('date_added'),
            item.get('rating'),
            item.get('folder_id'),
            self.normalize_string(artist)[:300],
            self.normalize_string(album_title)[:500],
            self.create_match_key(artist, album_title)[:500],
            stats.get('num_for_sale') if stats else None,
            lowest_price,
            basic.get('thumb'),
            basic.get('cover_image'),
            media_condition[:100],
            sleeve_condition[:100]
        ))
        
        # Return the collection ID for track insertion
        self.execute("SELECT id FROM discogs_collection WHERE release_id = %s", (item.get('id'),))
        result = self.fetch_one()
        return result['id'] if result else None
    
    def insert_discogs_track(self, collection_id, release_id, track):
        """Insert a Discogs track"""
        self.execute("""
            INSERT INTO discogs_tracks 
                (collection_id, release_id, position, track_title, duration, track_artists, extra_artists)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            collection_id,
            release_id,
            track.get('position', '')[:20],
            track.get('title', '')[:500],
            track.get('duration', '')[:20],
            str(track.get('artists', ''))[:500] if track.get('artists') else None,
            str(track.get('extraartists', ''))[:500] if track.get('extraartists') else None
        ))
    
    def insert_discogs_wantlist(self, item):
        """Insert or update a Discogs wantlist item"""
        basic = item.get('basic_information', {})
        artist = basic['artists'][0]['name'] if basic.get('artists') else 'Unknown'
        album_title = basic.get('title', 'Unknown')
        
        # Get marketplace stats
        stats = item.get('marketplace_stats', {})
        lowest_price = None
        num_for_sale = 0
        if stats:
            if stats.get('lowest_price'):
                lowest_price = stats['lowest_price'].get('value')
            num_for_sale = stats.get('num_for_sale', 0)
        
        self.execute("""
            INSERT INTO discogs_wantlist 
                (release_id, artist, album_title, label, format, year,
                 date_added, notes, num_for_sale, lowest_price, available,
                 marketplace_url, thumb_url, cover_image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                num_for_sale = VALUES(num_for_sale),
                lowest_price = VALUES(lowest_price),
                available = VALUES(available),
                updated_at = NOW()
        """, (
            item.get('id'),
            artist[:300],
            album_title[:500],
            basic['labels'][0]['name'][:300] if basic.get('labels') else None,
            basic['formats'][0]['name'][:100] if basic.get('formats') else None,
            basic.get('year'),
            item.get('date_added'),
            item.get('notes'),
            num_for_sale,
            lowest_price,
            num_for_sale > 0,
            f"https://www.discogs.com/sell/release/{item.get('id')}",
            basic.get('thumb'),
            basic.get('cover_image')
        ))


# Test connection when run directly
if __name__ == "__main__":
    db = MusicDB()
    if db.connect():
        # Test query
        db.execute("SELECT COUNT(*) as cnt FROM keep_track")
        result = db.fetch_one()
        print(f"keep_track has {result['cnt']} records")
        db.disconnect()
