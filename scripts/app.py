#!/usr/bin/env python3
"""
app.py - Flask API for Music Collection Manager
Provides REST endpoints for HTML pages to interact with MySQL database
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
from datetime import datetime
from db_helper import MusicDB

app = Flask(__name__)
CORS(app)  # Allow cross-origin requests from HTML pages

# =============================================================
# HELPER FUNCTIONS
# =============================================================

def get_db():
    """Get database connection"""
    db = MusicDB()
    if not db.connect():
        return None
    return db

def success_response(data=None, message=None):
    """Standard success response"""
    response = {'status': 'success'}
    if data is not None:
        response['data'] = data
    if message:
        response['message'] = message
    return jsonify(response)

def error_response(message, status_code=400):
    """Standard error response"""
    return jsonify({'status': 'error', 'message': message}), status_code

# =============================================================
# SEARCH ENDPOINTS
# =============================================================

@app.route('/api/search', methods=['GET'])
def search_albums():
    """
    Search albums across collections
    Query params:
        q: search query (required)
        source: 'roon', 'discogs', 'all' (default: 'all')
        limit: max results (default: 50, max: 500)
        offset: starting position (default: 0)
    """
    query = request.args.get('q', '').strip()
    source = request.args.get('source', 'all')
    limit = min(int(request.args.get('limit', 50)), 5000)
    offset = int(request.args.get('offset', 0))
    
    if not query:
        return error_response('Search query required')
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    results = []
    total = 0
    search_pattern = f'%{query}%'
    
    try:
        # Search Discogs collection
        if source in ('discogs', 'all'):
            # Get count first
            db.execute("""
                SELECT COUNT(*) as cnt FROM discogs_collection
                WHERE artist LIKE %s OR album_title LIKE %s
            """, (search_pattern, search_pattern))
            discogs_count = db.fetch_one()['cnt']
            
            if source == 'discogs':
                total = discogs_count
                db.execute("""
                    SELECT id, artist, album_title, label, format, year, 
                           thumb_url, last_listened, is_nun, 'discogs' as source
                    FROM discogs_collection
                    WHERE artist LIKE %s OR album_title LIKE %s
                    ORDER BY artist, album_title
                    LIMIT %s OFFSET %s
                """, (search_pattern, search_pattern, limit, offset))
                results.extend(db.fetch_all())
        
        # Search Roon albums
        if source in ('roon', 'all'):
            # Get count first
            db.execute("""
                SELECT COUNT(*) as cnt FROM roon_albums
                WHERE artist LIKE %s OR album_title LIKE %s
            """, (search_pattern, search_pattern))
            roon_count = db.fetch_one()['cnt']
            
            if source == 'roon':
                total = roon_count
                db.execute("""
                    SELECT id, artist, album_title, image_key, 'roon' as source
                    FROM roon_albums
                    WHERE artist LIKE %s OR album_title LIKE %s
                    ORDER BY artist, album_title
                    LIMIT %s OFFSET %s
                """, (search_pattern, search_pattern, limit, offset))
                results.extend(db.fetch_all())
        
        # Combined search (source='all')
        if source == 'all':
            total = discogs_count + roon_count
            
            # For combined, we do a UNION query with pagination
            db.execute("""
                SELECT * FROM (
                    SELECT id, artist, album_title, label, format, year, 
                           thumb_url, 'discogs' as source
                    FROM discogs_collection
                    WHERE artist LIKE %s OR album_title LIKE %s
                    UNION ALL
                    SELECT id, artist, album_title, NULL as label, NULL as format, NULL as year,
                           NULL as thumb_url, 'roon' as source
                    FROM roon_albums
                    WHERE artist LIKE %s OR album_title LIKE %s
                ) combined
                ORDER BY artist, album_title
                LIMIT %s OFFSET %s
            """, (search_pattern, search_pattern, search_pattern, search_pattern, limit, offset))
            results = db.fetch_all()
        
        db.disconnect()
        return success_response({
            'items': results,
            'total': total,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + limit) < total
        })
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/unified/collection', methods=['GET'])
def get_unified_collection():
    """Get unified collection from both Roon and Discogs
    
    Query params:
        source: 'roon', 'discogs', or '' for all
        hide_dupes: 'true' (default) or 'false' - hide Roon albums tagged as physical dupes
    """
    limit = min(int(request.args.get('limit', 100)), 15000)
    offset = int(request.args.get('offset', 0))
    source_filter = request.args.get('source', '')  # 'roon', 'discogs', or '' for all
    hide_dupes = request.args.get('hide_dupes', 'true').lower() != 'false'
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        # Build WHERE clause for Roon based on hide_dupes
        roon_where = "WHERE is_physical_dupe = FALSE" if hide_dupes else ""
        
        # Build query based on filter
        if source_filter == 'roon':
            # Roon only
            count_query = f"SELECT COUNT(*) as cnt FROM roon_albums {roon_where}"
            data_query = f"""
                SELECT 'roon' as source, id, artist, album_title, 
                       NULL as label, 'FLAC' as format, NULL as year,
                       NULL as date_added, NULL as thumb_url,
                       NULL as media_condition, NULL as sleeve_condition,
                       NULL as last_listened, 0 as is_nun, NULL as notes,
                       is_physical_dupe, physical_tag
                FROM roon_albums
                {roon_where}
                ORDER BY artist, album_title
                LIMIT %s OFFSET %s
            """
            db.execute(count_query)
            total = db.fetch_one()['cnt']
            db.execute(data_query, (limit, offset))
            
        elif source_filter == 'discogs':
            # Discogs only
            count_query = "SELECT COUNT(*) as cnt FROM discogs_collection"
            data_query = """
                SELECT 'discogs' as source, id, artist, album_title,
                       label, format, year, date_added, thumb_url,
                       media_condition, sleeve_condition, last_listened,
                       is_nun, notes,
                       FALSE as is_physical_dupe, NULL as physical_tag
                FROM discogs_collection
                ORDER BY artist, album_title
                LIMIT %s OFFSET %s
            """
            db.execute(count_query)
            total = db.fetch_one()['cnt']
            db.execute(data_query, (limit, offset))
            
        else:
            # Both - union query
            count_query = f"""
                SELECT 
                    (SELECT COUNT(*) FROM roon_albums {roon_where}) + 
                    (SELECT COUNT(*) FROM discogs_collection) as cnt
            """
            db.execute(count_query)
            total = db.fetch_one()['cnt']
            
            data_query = f"""
                (SELECT 'roon' as source, id, artist, album_title, 
                        NULL as label, 'FLAC' as format, NULL as year,
                        NULL as date_added, NULL as thumb_url,
                        NULL as media_condition, NULL as sleeve_condition,
                        NULL as last_listened, 0 as is_nun, NULL as notes,
                        is_physical_dupe, physical_tag
                 FROM roon_albums
                 {roon_where})
                UNION ALL
                (SELECT 'discogs' as source, id, artist, album_title,
                        label, format, year, date_added, thumb_url,
                        media_condition, sleeve_condition, last_listened,
                        is_nun, notes,
                        FALSE as is_physical_dupe, NULL as physical_tag
                 FROM discogs_collection)
                ORDER BY artist, album_title
                LIMIT %s OFFSET %s
            """
            db.execute(data_query, (limit, offset))
        
        results = db.fetch_all()
        
        # Get counts for stats
        db.execute("SELECT COUNT(*) as cnt FROM roon_albums")
        roon_total = db.fetch_one()['cnt']
        db.execute("SELECT COUNT(*) as cnt FROM roon_albums WHERE is_physical_dupe = FALSE")
        roon_unique = db.fetch_one()['cnt']
        db.execute("SELECT COUNT(*) as cnt FROM roon_albums WHERE is_physical_dupe = TRUE")
        roon_dupes = db.fetch_one()['cnt']
        db.execute("SELECT COUNT(*) as cnt FROM discogs_collection")
        discogs_count = db.fetch_one()['cnt']
        
        # Use filtered or total based on hide_dupes
        roon_count = roon_unique if hide_dupes else roon_total
        
        db.disconnect()
        return success_response({
            'items': results,
            'total': total,
            'limit': limit,
            'offset': offset,
            'has_more': offset + len(results) < total,
            'hide_dupes': hide_dupes,
            'counts': {
                'roon': roon_count,
                'roon_total': roon_total,
                'roon_dupes': roon_dupes,
                'discogs': discogs_count,
                'total': roon_count + discogs_count
            }
        })
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/discogs/collection', methods=['GET'])
def get_discogs_collection():
    """Get full Discogs collection with optional pagination"""
    limit = min(int(request.args.get('limit', 100)), 5000)
    offset = int(request.args.get('offset', 0))
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            SELECT id, release_id, artist, album_title, label, format, year,
                   date_added, thumb_url, media_condition, sleeve_condition,
                   last_listened, is_nun, notes
            FROM discogs_collection
            ORDER BY artist, album_title
            LIMIT %s OFFSET %s
        """, (limit, offset))
        results = db.fetch_all()
        
        db.execute("SELECT COUNT(*) as total FROM discogs_collection")
        total = db.fetch_one()['total']
        
        db.disconnect()
        return success_response({'items': results, 'total': total, 'limit': limit, 'offset': offset})
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/discogs/wantlist', methods=['GET'])
def get_discogs_wantlist():
    """Get full Discogs wantlist"""
    limit = min(int(request.args.get('limit', 100)), 2000)
    offset = int(request.args.get('offset', 0))
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            SELECT id, release_id, artist, album_title, label, format, year,
                   lowest_price, num_for_sale, available, marketplace_url, thumb_url
            FROM discogs_wantlist
            ORDER BY artist, album_title
            LIMIT %s OFFSET %s
        """, (limit, offset))
        results = db.fetch_all()
        
        db.execute("SELECT COUNT(*) as total FROM discogs_wantlist")
        total = db.fetch_one()['total']
        
        db.disconnect()
        return success_response({'items': results, 'total': total, 'limit': limit, 'offset': offset})
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/roon/albums', methods=['GET'])
def get_roon_albums():
    """Get Roon albums with optional pagination"""
    limit = min(int(request.args.get('limit', 100)), 10000)
    offset = int(request.args.get('offset', 0))
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            SELECT id, artist, album_title, image_key
            FROM roon_albums
            ORDER BY artist, album_title
            LIMIT %s OFFSET %s
        """, (limit, offset))
        results = db.fetch_all()
        
        db.execute("SELECT COUNT(*) as total FROM roon_albums")
        total = db.fetch_one()['total']
        
        db.disconnect()
        return success_response({'items': results, 'total': total, 'limit': limit, 'offset': offset})
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

# =============================================================
# LISTENING HISTORY ENDPOINTS
# =============================================================

@app.route('/api/listening_history', methods=['GET'])
def get_listening_history():
    """Get listening history entries"""
    limit = min(int(request.args.get('limit', 50)), 200)
    offset = int(request.args.get('offset', 0))
    source_filter = request.args.get('source')  # 'roon', 'discogs', or None for all
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        if source_filter:
            db.execute("""
                SELECT id, artist, album, source, listened_at, format, notes,
                       roon_album_id, discogs_collection_id
                FROM listening_history
                WHERE source = %s
                ORDER BY listened_at DESC
                LIMIT %s OFFSET %s
            """, (source_filter, limit, offset))
        else:
            db.execute("""
                SELECT id, artist, album, source, listened_at, format, notes,
                       roon_album_id, discogs_collection_id
                FROM listening_history
                ORDER BY listened_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
        
        results = db.fetch_all()
        
        # Convert datetime to string for JSON
        for row in results:
            if row.get('listened_at'):
                row['listened_at'] = row['listened_at'].strftime('%Y-%m-%d %H:%M:%S')
        
        db.disconnect()
        return success_response(results)
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/listening_history', methods=['POST'])
def add_listening_entry():
    """
    Add a new listening history entry
    Body: {
        artist: string (required),
        album: string (required),
        source: 'roon' | 'discogs' | 'both' (required),
        listened_at: datetime string (optional, defaults to now),
        format: string (optional),
        notes: string (optional),
        roon_album_id: int (optional),
        discogs_collection_id: int (optional)
    }
    """
    data = request.get_json()
    
    if not data:
        return error_response('Request body required')
    
    artist = data.get('artist', '').strip()
    album = data.get('album', '').strip()
    source = data.get('source', '').strip()
    
    if not artist or not album:
        return error_response('Artist and album are required')
    
    if source not in ('roon', 'discogs', 'both'):
        return error_response('Source must be roon, discogs, or both')
    
    # Parse listened_at or default to now
    listened_at = data.get('listened_at')
    if listened_at:
        try:
            listened_at = datetime.strptime(listened_at, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            try:
                listened_at = datetime.strptime(listened_at, '%Y-%m-%d')
            except ValueError:
                return error_response('Invalid date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS')
    else:
        listened_at = datetime.now()
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            INSERT INTO listening_history 
                (artist, album, source, listened_at, format, notes, roon_album_id, discogs_collection_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            artist,
            album,
            source,
            listened_at,
            data.get('format'),
            data.get('notes'),
            data.get('roon_album_id'),
            data.get('discogs_collection_id')
        ))
        db.commit()
        
        # Also update last_listened on discogs_collection if applicable
        if source in ('discogs', 'both') and data.get('discogs_collection_id'):
            db.execute("""
                UPDATE discogs_collection 
                SET last_listened = %s 
                WHERE id = %s
            """, (listened_at, data.get('discogs_collection_id')))
            db.commit()
        
        db.disconnect()
        return success_response(message='Listening entry added')
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

# =============================================================
# UPDATE ENDPOINTS
# =============================================================

@app.route('/api/discogs/collection/<int:id>/last_listened', methods=['PUT'])
def update_last_listened(id):
    """Update last_listened date for a Discogs collection item"""
    data = request.get_json()
    
    if not data or 'last_listened' not in data:
        return error_response('last_listened is required')
    
    try:
        last_listened = datetime.strptime(data['last_listened'], '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            last_listened = datetime.strptime(data['last_listened'], '%Y-%m-%d')
        except ValueError:
            return error_response('Invalid date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS')
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            UPDATE discogs_collection 
            SET last_listened = %s 
            WHERE id = %s
        """, (last_listened, id))
        db.commit()
        db.disconnect()
        return success_response(message='Last listened updated')
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/discogs/collection/<int:id>/is_nun', methods=['PUT'])
def update_is_nun(id):
    """Update is_nun flag for a Discogs collection item"""
    data = request.get_json()
    
    if not data or 'is_nun' not in data:
        return error_response('is_nun is required')
    
    is_nun = bool(data['is_nun'])
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            UPDATE discogs_collection 
            SET is_nun = %s 
            WHERE id = %s
        """, (is_nun, id))
        db.commit()
        db.disconnect()
        return success_response(message='is_nun updated')
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/discogs/collection/<int:id>/notes', methods=['PUT'])
def update_discogs_notes(id):
    """Update notes for a Discogs collection item"""
    data = request.get_json()
    
    if not data:
        return error_response('Request body required')
    
    notes = data.get('notes', '')
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            UPDATE discogs_collection 
            SET notes = %s 
            WHERE id = %s
        """, (notes, id))
        db.commit()
        db.disconnect()
        return success_response(message='Notes updated')
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/roon/play_history/<int:id>/played_at', methods=['PUT'])
def update_roon_played_at(id):
    """Update played_at timestamp for a Roon play history record"""
    data = request.get_json()
    
    if not data or 'played_at' not in data:
        return error_response('played_at is required')
    
    try:
        played_at = datetime.strptime(data['played_at'], '%Y-%m-%d %H:%M:%S')
    except ValueError:
        try:
            played_at = datetime.strptime(data['played_at'], '%Y-%m-%d')
        except ValueError:
            return error_response('Invalid date format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS')
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            UPDATE roon_play_history 
            SET played_at = %s 
            WHERE id = %s
        """, (played_at, id))
        db.commit()
        db.disconnect()
        return success_response(message='Played at updated')
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

# =============================================================
# BOOTLEGS ENDPOINTS (Live recordings with date pattern)
# =============================================================

@app.route('/api/roon/bootlegs', methods=['GET'])
def get_bootlegs():
    """
    Get Roon albums that match bootleg date pattern (YYYY MM/DD)
    Query params:
        artist: filter by artist (optional)
        limit: max results (default: 200)
        offset: starting position (default: 0)
    """
    artist_filter = request.args.get('artist', '').strip()
    limit = min(int(request.args.get('limit', 500)), 10000)
    offset = int(request.args.get('offset', 0))
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        # Pattern: starts with YYYY MM/DD (e.g., "1974 06/26")
        if artist_filter:
            db.execute("""
                SELECT COUNT(*) as cnt FROM roon_albums
                WHERE album_title REGEXP '^[0-9]{4} [0-9]{2}/[0-9]{2}'
                AND artist LIKE %s
            """, (f'%{artist_filter}%',))
        else:
            db.execute("""
                SELECT COUNT(*) as cnt FROM roon_albums
                WHERE album_title REGEXP '^[0-9]{4} [0-9]{2}/[0-9]{2}'
            """)
        total = db.fetch_one()['cnt']
        
        if artist_filter:
            db.execute("""
                SELECT id, artist, album_title, image_key,
                       SUBSTRING(album_title, 1, 10) as show_date
                FROM roon_albums
                WHERE album_title REGEXP '^[0-9]{4} [0-9]{2}/[0-9]{2}'
                AND artist LIKE %s
                ORDER BY artist, album_title
                LIMIT %s OFFSET %s
            """, (f'%{artist_filter}%', limit, offset))
        else:
            db.execute("""
                SELECT id, artist, album_title, image_key,
                       SUBSTRING(album_title, 1, 10) as show_date
                FROM roon_albums
                WHERE album_title REGEXP '^[0-9]{4} [0-9]{2}/[0-9]{2}'
                ORDER BY artist, album_title
                LIMIT %s OFFSET %s
            """, (limit, offset))
        
        results = db.fetch_all()
        
        db.disconnect()
        return success_response({
            'items': results,
            'total': total,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + limit) < total
        })
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/roon/bootlegs/artists', methods=['GET'])
def get_bootleg_artists():
    """Get list of artists with bootleg recordings and their counts"""
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            SELECT artist, COUNT(*) as show_count
            FROM roon_albums
            WHERE album_title REGEXP '^[0-9]{4} [0-9]{2}/[0-9]{2}'
            GROUP BY artist
            ORDER BY show_count DESC
        """)
        results = db.fetch_all()
        
        db.disconnect()
        return success_response(results)
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/roon/tracks', methods=['GET'])
def get_roon_tracks():
    """Get tracks for a specific album
    
    Query params:
        album: Album title to match (required)
        album_artist: Album artist to match (optional, for disambiguation)
    """
    album = request.args.get('album', '').strip()
    album_artist = request.args.get('album_artist', '').strip()
    
    if not album:
        return error_response('Album parameter required')
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        if album_artist:
            db.execute("""
                SELECT track_number, disc_number, track_title, track_artists, 
                       album, album_artist, composers, source
                FROM roon_tracks
                WHERE album = %s AND album_artist = %s
                ORDER BY disc_number, track_number
            """, (album, album_artist))
        else:
            db.execute("""
                SELECT track_number, disc_number, track_title, track_artists,
                       album, album_artist, composers, source
                FROM roon_tracks
                WHERE album = %s
                ORDER BY disc_number, track_number
            """, (album,))
        
        results = db.fetch_all()
        
        db.disconnect()
        return success_response({
            'tracks': results,
            'count': len(results),
            'album': album
        })
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

# =============================================================
# STATS ENDPOINTS
# =============================================================

@app.route('/api/stats/overview', methods=['GET'])
def get_stats_overview():
    """Get overview statistics"""
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        stats = {}
        
        # Roon stats
        db.execute("SELECT COUNT(*) as cnt FROM roon_albums")
        stats['roon_albums'] = db.fetch_one()['cnt']
        
        db.execute("SELECT COUNT(*) as cnt FROM roon_tracks")
        stats['roon_tracks'] = db.fetch_one()['cnt']
        
        db.execute("SELECT COUNT(*) as cnt FROM roon_play_history")
        stats['roon_plays'] = db.fetch_one()['cnt']
        
        # Discogs stats
        db.execute("SELECT COUNT(*) as cnt FROM discogs_collection")
        stats['discogs_collection'] = db.fetch_one()['cnt']
        
        db.execute("SELECT COUNT(*) as cnt FROM discogs_wantlist")
        stats['discogs_wantlist'] = db.fetch_one()['cnt']
        
        db.execute("SELECT SUM(lowest_price) as total FROM discogs_wantlist WHERE lowest_price IS NOT NULL")
        result = db.fetch_one()
        stats['wantlist_total_value'] = float(result['total']) if result['total'] else 0
        
        # Listening history
        db.execute("SELECT COUNT(*) as cnt FROM listening_history")
        stats['listening_entries'] = db.fetch_one()['cnt']
        
        # Albums in both collections
        db.execute("""
            SELECT COUNT(*) as cnt 
            FROM roon_albums r 
            INNER JOIN discogs_collection d ON r.match_key = d.match_key
        """)
        stats['albums_in_both'] = db.fetch_one()['cnt']
        
        # Nun collection count
        db.execute("SELECT COUNT(*) as cnt FROM discogs_collection WHERE is_nun = TRUE")
        stats['nun_albums'] = db.fetch_one()['cnt']
        
        db.disconnect()
        return success_response(stats)
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/stats/play_counts', methods=['GET'])
def get_play_counts():
    """Get album play counts from Roon history"""
    limit = min(int(request.args.get('limit', 50)), 200)
    
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            SELECT album_artist as artist, album, COUNT(*) as play_count
            FROM roon_play_history
            GROUP BY album_artist, album
            ORDER BY play_count DESC
            LIMIT %s
        """, (limit,))
        results = db.fetch_all()
        
        db.disconnect()
        return success_response(results)
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

@app.route('/api/stats/live_matches', methods=['GET'])
def get_live_matches():
    """Get live show matches (bootleg vs official)"""
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    try:
        db.execute("""
            SELECT * FROM live_show_matches
            ORDER BY show_date DESC
        """)
        results = db.fetch_all()
        
        # Convert dates to strings
        for row in results:
            if row.get('show_date'):
                row['show_date'] = row['show_date'].strftime('%Y-%m-%d')
        
        db.disconnect()
        return success_response(results)
        
    except Exception as e:
        db.disconnect()
        return error_response(str(e), 500)

# =============================================================
# HEALTH CHECK
# =============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    db = get_db()
    if not db:
        return error_response('Database connection failed', 500)
    
    db.execute("SELECT 1 as ok")
    result = db.fetch_one()
    db.disconnect()
    return success_response(message='OK')

# =============================================================
# RUN SERVER
# =============================================================

if __name__ == '__main__':
    print("="*60)
    print("Music Collection API Server")
    print("="*60)
    print("Endpoints:")
    print("  GET  /api/search?q=...&source=...")
    print("  GET  /api/discogs/collection")
    print("  GET  /api/discogs/wantlist")
    print("  GET  /api/roon/albums")
    print("  GET  /api/listening_history")
    print("  POST /api/listening_history")
    print("  PUT  /api/discogs/collection/<id>/last_listened")
    print("  PUT  /api/discogs/collection/<id>/is_nun")
    print("  PUT  /api/discogs/collection/<id>/notes")
    print("  PUT  /api/roon/play_history/<id>/played_at")
    print("  GET  /api/stats/overview")
    print("  GET  /api/stats/play_counts")
    print("  GET  /api/health")
    print("="*60)
    print("Starting server on http://localhost:5001")
    print("="*60)
    
    app.run(host='0.0.0.0', port=5001, debug=True)
