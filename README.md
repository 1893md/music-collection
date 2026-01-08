# Music Collection Manager

A unified music collection management system that combines your digital library (Roon) with your physical collection (Discogs) into a single, searchable database with a web interface.

## Features

- **Unified Collection View**: Browse both digital (Roon) and physical (Discogs) collections with duplicate filtering
- **Physical Duplicate Detection**: Automatically flags Roon albums tagged with myCDs/mYLps as physical duplicates
- **Wantlist Management**: Track albums you want with real-time marketplace prices
- **Bootleg Archive**: Special handling for live recordings with expandable setlists
- **Sortable Columns**: Click column headers to sort by any field
- **Listening History**: Log what you've been listening to across both formats
- **Live Show Matching**: Identify overlaps between bootleg recordings and official releases
- **REST API**: Full API for custom integrations
- **Web Interface**: Modern, responsive HTML pages

## Architecture

```
┌─────────────────┐     ┌─────────────────┐
│   Roon Core     │     │    Discogs      │
│   (Digital)     │     │   (Physical)    │
└────────┬────────┘     └────────┬────────┘
         │                       │
         └───────────┬───────────┘
                     │
              ┌──────▼──────┐
              │   MySQL DB   │
              │music_collection│
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │  Flask API   │
              │  (port 5001) │
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │   Apache     │
              │ (ProxyPass)  │
              └──────┬──────┘
                     │
              ┌──────▼──────┐
              │ Web Browser  │
              └─────────────┘
```

## Prerequisites

- Python 3.10+
- MySQL 8.0+
- Apache with mod_proxy (optional, for web interface)
- Roon Core with API access
- Discogs account with API token

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/music-collection.git
cd music-collection
```

### 2. Create virtual environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Set up MySQL database

```bash
mysql -u root -p
```

```sql
CREATE DATABASE music_collection CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'music_app'@'localhost' IDENTIFIED BY 'your_password_here';
GRANT ALL PRIVILEGES ON music_collection.* TO 'music_app'@'localhost';
FLUSH PRIVILEGES;
```

```bash
mysql -u music_app -p music_collection < sql/schema.sql
```

### 4. Configure environment

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 5. Update file paths in database

Update the `keep_track` table with your actual file paths:

```sql
UPDATE keep_track SET file_path = '/path/to/your/LibraryTracks-complete.csv' WHERE source_name = 'roon_tracks';
UPDATE keep_track SET file_path = '/path/to/your/roon_history.json' WHERE source_name = 'roon_play_history';
```

### 6. Run initial sync

```bash
cd scripts
python sync_all.py
```

### 7. Start the API server

```bash
python app.py
```

The API will be available at `http://localhost:5001`

### 8. Set up web interface (optional)

Copy HTML files to your web server:

```bash
cp html/*.html /path/to/your/webserver/htmls/
cp html/index.html /path/to/your/webserver/
```

Configure Apache proxy (add to httpd.conf):

```apache
ProxyPass /api http://localhost:5001/api
ProxyPassReverse /api http://localhost:5001/api
```

## Usage

### Sync Commands

```bash
# Sync everything
python scripts/sync_all.py

# Sync specific source
python scripts/sync_all.py --source roon_albums
python scripts/sync_all.py --source roon_tags        # Sync physical duplicate tags
python scripts/sync_all.py --source roon_tracks
python scripts/sync_all.py --source roon_play_history
python scripts/sync_all.py --source discogs_collection
python scripts/sync_all.py --source discogs_wantlist

# Force sync (ignore 7-day skip)
python scripts/sync_all.py --force
python scripts/sync_all.py --source discogs_wantlist --force
```

### Environment Variables

```bash
export DISCOGS_TOKEN="your_token_here"
export DISCOGS_USERNAME="your_username"  # defaults to 1893md
```

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check |
| `/api/search?q=...` | GET | Search all collections |
| `/api/unified/collection` | GET | Unified collection (Roon + Discogs, filtered) |
| `/api/discogs/collection` | GET | Get Discogs collection |
| `/api/discogs/wantlist` | GET | Get Discogs wantlist |
| `/api/roon/albums` | GET | Get Roon albums |
| `/api/roon/bootlegs` | GET | Get bootleg recordings |
| `/api/roon/tracks?album=...` | GET | Get tracks for an album |
| `/api/listening_history` | GET/POST | Get or add listening history |
| `/api/stats/overview` | GET | Get collection statistics |

### Web Interface

- **/** - Home page with collection overview
- **/htmls/collection.html** - Unified collection browser (sortable, with duplicate toggle)
- **/htmls/wants.html** - Browse and track wantlist with marketplace prices
- **/htmls/bootlegs.html** - Browse bootleg recordings with expandable setlists
- **/htmls/listening_history.html** - Log listening sessions

#### Bootlegs Page Features
- Click any show to expand and view the full setlist
- Tracks are loaded from the roon_tracks table
- Multi-disc shows display disc separators
- Setlist data cached for quick re-open

## Bootleg Pattern Matching

The system automatically identifies bootleg recordings by matching the pattern `YYYY MM/DD` at the start of album titles. For example:

- `1974 06/26 Providence Civic Center` → Recognized as June 26, 1974 show
- `1977 05/08 Barton Hall, Cornell University` → Recognized as May 8, 1977 show

## Physical Duplicate Detection

Albums in Roon tagged with `myCDs` or `mYLps` are flagged as physical duplicates. This allows the unified collection view to:

- Show only unique albums by default (hiding Roon copies of physical albums)
- Toggle to show all albums including duplicates
- Track which digital albums you also own physically

To use this feature, tag your Roon albums with `myCDs` or `mYLps` tags, then run:

```bash
python scripts/sync_all.py --source roon_tags
```

## Data Sources

### Roon
- Albums synced via Roon API
- Tracks imported from CSV export
- Play history imported from JSON export

### Discogs
- Collection synced via API with marketplace values
- Wantlist synced via API with availability tracking
- Tracklists fetched for each release

## Project Structure

```
music-collection/
├── scripts/
│   ├── app.py           # Flask API server
│   ├── db_helper.py     # Database connection helper
│   └── sync_all.py      # Data sync script
├── html/
│   ├── index.html       # Home page
│   ├── collection.html  # Collection browser
│   ├── wants.html       # Wantlist browser
│   ├── bootlegs.html    # Bootleg browser
│   └── listening_history.html
├── sql/
│   └── schema.sql       # Database schema
├── .env.example         # Environment template
├── requirements.txt     # Python dependencies
└── README.md
```

## License

MIT License - See LICENSE file for details.

## Acknowledgments

- [Roon](https://roon.app/) for the excellent music management system
- [Discogs](https://www.discogs.com/) for the comprehensive music database
- [roonapi](https://github.com/pavoni/pyrern) Python library for Roon integration
