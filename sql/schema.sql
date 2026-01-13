-- =============================================================
-- Music Collection Database Schema
-- MySQL 8.0+
-- =============================================================

-- Create database (run as root)
-- CREATE DATABASE music_collection CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
-- CREATE USER 'music_app'@'localhost' IDENTIFIED BY 'your_password_here';
-- GRANT ALL PRIVILEGES ON music_collection.* TO 'music_app'@'localhost';
-- FLUSH PRIVILEGES;

USE music_collection;

-- =============================================================
-- ROON TABLES
-- =============================================================

CREATE TABLE IF NOT EXISTS roon_albums (
    id INT AUTO_INCREMENT PRIMARY KEY,
    album_title VARCHAR(500) NOT NULL,
    artist VARCHAR(300),
    image_key VARCHAR(100),
    item_key VARCHAR(50),
    artist_norm VARCHAR(300),
    album_norm VARCHAR(500),
    match_key VARCHAR(500),
    is_physical_dupe BOOLEAN DEFAULT FALSE,
    physical_tag VARCHAR(50) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_artist (artist),
    INDEX idx_match_key (match_key),
    INDEX idx_album_title (album_title),
    INDEX idx_is_physical_dupe (is_physical_dupe),
    UNIQUE KEY unique_item_key (item_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roon_tracks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    album_artist VARCHAR(300),
    album VARCHAR(500),
    disc_number INT,
    track_number INT,
    track_title VARCHAR(500),
    track_artists VARCHAR(500),
    composers VARCHAR(500),
    external_id VARCHAR(100),
    source VARCHAR(50),
    is_duplicate BOOLEAN DEFAULT FALSE,
    is_hidden BOOLEAN DEFAULT FALSE,
    tags TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_album_artist (album_artist),
    INDEX idx_album (album)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roon_play_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    album_artist VARCHAR(300),
    album VARCHAR(500),
    disc_number INT,
    track_number INT,
    track_title VARCHAR(500),
    track_artists VARCHAR(500),
    composers VARCHAR(500),
    external_id VARCHAR(100),
    source VARCHAR(50),
    played_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_album_artist (album_artist),
    INDEX idx_album (album),
    INDEX idx_played_at (played_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================
-- DISCOGS TABLES
-- =============================================================

CREATE TABLE IF NOT EXISTS discogs_collection (
    id INT AUTO_INCREMENT PRIMARY KEY,
    release_id INT NOT NULL,
    instance_id INT,
    artist VARCHAR(300),
    album_title VARCHAR(500),
    label VARCHAR(300),
    format VARCHAR(100),
    year INT,
    date_added DATETIME,
    rating INT,
    folder_id INT,
    artist_norm VARCHAR(300),
    album_norm VARCHAR(500),
    match_key VARCHAR(500),
    num_for_sale INT,
    lowest_price DECIMAL(10,2),
    thumb_url VARCHAR(500),
    cover_image_url VARCHAR(500),
    media_condition VARCHAR(100),
    sleeve_condition VARCHAR(100),
    last_listened DATETIME NULL,
    is_nun BOOLEAN DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_release (release_id),
    INDEX idx_artist (artist),
    INDEX idx_match_key (match_key),
    INDEX idx_is_nun (is_nun)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS discogs_tracks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    collection_id INT NOT NULL,
    release_id INT NOT NULL,
    position VARCHAR(20),
    track_title VARCHAR(500),
    duration VARCHAR(20),
    track_artists VARCHAR(500),
    extra_artists VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (collection_id) REFERENCES discogs_collection(id) ON DELETE CASCADE,
    INDEX idx_release_id (release_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS discogs_wantlist (
    id INT AUTO_INCREMENT PRIMARY KEY,
    release_id INT NOT NULL,
    artist VARCHAR(300),
    album_title VARCHAR(500),
    label VARCHAR(300),
    format VARCHAR(100),
    year INT,
    date_added DATETIME,
    notes TEXT,
    num_for_sale INT DEFAULT 0,
    lowest_price DECIMAL(10,2),
    available BOOLEAN DEFAULT FALSE,
    marketplace_url VARCHAR(500),
    thumb_url VARCHAR(500),
    cover_image_url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_release (release_id),
    INDEX idx_artist (artist),
    INDEX idx_available (available)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================
-- USER DATA TABLES
-- =============================================================

CREATE TABLE IF NOT EXISTS listening_history (
    id INT AUTO_INCREMENT PRIMARY KEY,
    artist VARCHAR(300),
    album VARCHAR(500),
    source ENUM('roon', 'discogs', 'both') NOT NULL,
    listened_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,
    discogs_collection_id INT NULL,
    roon_album_id INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (discogs_collection_id) REFERENCES discogs_collection(id) ON DELETE SET NULL,
    FOREIGN KEY (roon_album_id) REFERENCES roon_albums(id) ON DELETE SET NULL,
    INDEX idx_listened_at (listened_at),
    INDEX idx_artist (artist)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS live_show_matches (
    id INT AUTO_INCREMENT PRIMARY KEY,
    show_date DATE,
    venue VARCHAR(500),
    artist VARCHAR(300),
    bootleg_album_title VARCHAR(500),
    official_album_title VARCHAR(500),
    roon_album_id INT NULL,
    discogs_collection_id INT NULL,
    match_type ENUM('exact', 'partial', 'manual') DEFAULT 'exact',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (roon_album_id) REFERENCES roon_albums(id) ON DELETE SET NULL,
    FOREIGN KEY (discogs_collection_id) REFERENCES discogs_collection(id) ON DELETE SET NULL,
    INDEX idx_show_date (show_date),
    INDEX idx_artist (artist)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================
-- SYNC TRACKING TABLE
-- =============================================================

CREATE TABLE IF NOT EXISTS keep_track (
    id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL UNIQUE,
    source_type ENUM('api', 'file') NOT NULL,
    file_path VARCHAR(500),
    last_sync DATETIME,
    records_count INT,
    sync_status VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Insert default sync tracking entries
INSERT INTO keep_track (source_name, source_type, file_path, last_sync) VALUES
('roon_albums', 'api', NULL, '2024-01-01 00:00:00'),
('roon_tracks', 'file', '/path/to/your/LibraryTracks-complete.csv', '2024-01-01 00:00:00'),
('roon_play_history', 'file', '/path/to/your/roon_history.json', '2024-01-01 00:00:00'),
('discogs_collection', 'api', NULL, '2024-01-01 00:00:00'),
('discogs_tracks', 'api', NULL, '2024-01-01 00:00:00'),
('discogs_wantlist', 'api', NULL, '2024-01-01 00:00:00')
ON DUPLICATE KEY UPDATE updated_at = NOW();

-- =============================================================
-- VIEWS
-- =============================================================

CREATE OR REPLACE VIEW unified_collection AS
SELECT 
    'discogs' as source,
    dc.id,
    dc.artist,
    dc.album_title,
    dc.year,
    dc.format,
    dc.match_key,
    dc.last_listened
FROM discogs_collection dc
UNION ALL
SELECT 
    'roon' as source,
    ra.id,
    ra.artist,
    ra.album_title,
    NULL as year,
    NULL as format,
    ra.match_key,
    NULL as last_listened
FROM roon_albums ra;

CREATE OR REPLACE VIEW album_play_counts AS
SELECT 
    album_artist as artist,
    album,
    COUNT(*) as play_count
FROM roon_play_history
GROUP BY album_artist, album
ORDER BY play_count DESC;

-- =============================================================
-- TRACK INDEX TABLE (for track browsing/cleanup)
-- =============================================================

CREATE TABLE IF NOT EXISTS track_index (
    id INT AUTO_INCREMENT PRIMARY KEY,
    track_title VARCHAR(500),
    album VARCHAR(500),
    artist VARCHAR(300),
    source ENUM('roon', 'discogs'),
    INDEX idx_track_title (track_title),
    INDEX idx_artist (artist),
    INDEX idx_album (album)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- =============================================================
-- SYNC HISTORY TABLE (track collection growth over time)
-- =============================================================

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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
