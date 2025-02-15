import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

S3_LOG_DATA = config['S3']['LOG_DATA']
S3_LOG_JSONPATH = config['S3']['LOG_JSONPATH']
S3_SONG_DATA = config['S3']['SONG_DATA']
IAM_ROLE_ARN = config['IAM']['ROLE_ARN']
S3_REGION = config['S3']['REGION']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR,
    auth          VARCHAR,
    firstName     VARCHAR,
    gender        VARCHAR,
    itemInSession BIGINT,
    lastName      VARCHAR,
    length        FLOAT,
    level         VARCHAR,
    location      VARCHAR,
    method        VARCHAR,
    page          VARCHAR,
    registration  FLOAT,
    sessionId     BIGINT,
    song          VARCHAR,
    status        INTEGER,
    ts            BIGINT,
    userAgent     VARCHAR,
    userId        BIGINT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs        INTEGER,
    artist_id        VARCHAR,
    artist_latitude  FLOAT,
    artist_longitude FLOAT,
    artist_location  VARCHAR,
    artist_name      VARCHAR,
    song_id          VARCHAR,
    title            VARCHAR,
    duration         FLOAT,
    year             SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY SORTKEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     BIGINT NOT NULL DISTKEY,
    level       VARCHAR NOT NULL,
    song_id     VARCHAR NOT NULL,
    artist_id   VARCHAR NOT NULL,
    session_id  BIGINT NOT NULL,
    location    VARCHAR,
    user_agent  VARCHAR
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id    BIGINT PRIMARY KEY SORTKEY,
    first_name VARCHAR NOT NULL,
    last_name  VARCHAR NOT NULL,
    gender     VARCHAR,
    level      VARCHAR
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR PRIMARY KEY SORTKEY,
    title     VARCHAR NOT NULL,
    artist_id VARCHAR NOT NULL DISTKEY,
    year      SMALLINT,
    duration  FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY SORTKEY,
    name      VARCHAR NOT NULL,
    location  VARCHAR,
    lattitude FLOAT,
    longitude FLOAT
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY DISTKEY SORTKEY,
    hour       SMALLINT,
    day        SMALLINT,
    week       SMALLINT,
    month      SMALLINT,
    year       SMALLINT,
    weekday    VARCHAR
);
""")

# STAGING TABLES

staging_events_copy = (f"""
COPY staging_events
FROM {S3_LOG_DATA}
IAM_ROLE {IAM_ROLE_ARN}
REGION {S3_REGION}
FORMAT AS JSON {S3_LOG_JSONPATH}
TIMEFORMAT AS 'epochmillisecs';
""")

staging_songs_copy = (f"""
COPY staging_songs
FROM {S3_SONG_DATA}
IAM_ROLE {IAM_ROLE_ARN}
REGION {S3_REGION}
FORMAT AS JSON 'auto';
""")

# FINAL TABLES

# Review comments:
# Distinct is not required for fact table, they can store duplicates
# Add a filter for 'duration=length'
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second', e.userId, e.level, s.song_id, s.artist_id, e.sessionId, s.artist_location, e.userAgent
FROM staging_events AS e
JOIN staging_songs as s ON (e.artist = s.artist_name AND e.song = s.title)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events WHERE (page = 'NextSong') AND (userId IS NOT NULL);
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs WHERE (song_id IS NOT NULL) AND (artist_id IS NOT NULL);
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, lattitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
                EXTRACT(hour FROM start_time)                     AS hour,
                EXTRACT(day FROM start_time)                      AS day,
                EXTRACT(week FROM start_time)                     AS week,
                EXTRACT(month FROM start_time)                    AS month,
                EXTRACT(year FROM start_time)                     AS year,
                EXTRACT(dayofweek FROM start_time)                AS weekday
FROM staging_events WHERE page = 'NextSong';
""")

# COUNT ROWS IN EACH TABLE
count_staging_events = ("""
    SELECT COUNT(*) FROM staging_events;
""")

count_rows_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs;
""")

count_rows_songplays = ("""
    SELECT COUNT(*) FROM songplays;
""")

count_rows_users = ("""
    SELECT COUNT(*) FROM users;
""")

count_rows_songs = ("""
    SELECT COUNT(*) FROM songs;
""")

count_rows_artists = ("""
    SELECT COUNT(*) FROM artists;
""")

count_rows_time = ("""
    SELECT COUNT(*) FROM time;
""")

five_most_playable_artists = {
    'question': 'What are the 5 most played artists?',
    'query': """
SELECT a.name, COUNT(sp.artist_id) AS number_of_plays
FROM songplays AS sp
JOIN artists as a ON (sp.artist_id = a.artist_id)
GROUP BY (a.name, sp.artist_id)
ORDER BY number_of_plays DESC
LIMIT 5;
    """
}

five_most_playable_songs = {
    'question': 'What are the five most played songs?',
    'query': """
SELECT a.name, s.title, COUNT(sp.song_id) AS number_of_plays
FROM songplays AS sp
JOIN artists as a ON (sp.artist_id = a.artist_id)
JOIN songs as s ON (sp.song_id = s.song_id)
GROUP BY (a.name, s.title)
ORDER BY number_of_plays DESC
LIMIT 5;
    """
}

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]

count_rows_queries = [
    count_staging_events,
    count_rows_staging_songs,
    count_rows_songplays,
    count_rows_users,
    count_rows_songs,
    count_rows_artists,
    count_rows_time
]

five_most_playable_queries = [
    five_most_playable_artists,
    five_most_playable_songs
]
