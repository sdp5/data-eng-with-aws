class SqlQueries:
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    ################### DATA QUALITY CHECKS ############################
    songplays_check_count = ("""
            SELECT COUNT(*) FROM songplays;
        """)

    songplays_check_nulls = ("""
            SELECT COUNT(*) FROM songplays
                    WHERE playid IS NULL OR
                          start_time IS NULL OR
                          userid IS NULL;
        """)

    users_check_count = ("""
            SELECT COUNT(*) FROM users;
        """)

    users_check_nulls = ("""
            SELECT COUNT(*) FROM users WHERE userid IS NULL;
        """)

    songs_check_count = ("""
            SELECT COUNT(*) FROM songs;
        """)

    songs_check_nulls = ("""
            SELECT COUNT(*) FROM songs WHERE songid IS NULL;
        """)

    artists_check_count = ("""
            SELECT COUNT(*) FROM artists;
        """)

    artists_check_nulls = ("""
            SELECT COUNT(*) FROM artists WHERE artistid IS NULL;
        """)

    time_check_count = ("""
            SELECT COUNT(*) FROM time;
        """)

    time_check_nulls = ("""
            SELECT COUNT(*) FROM time WHERE start_time IS NULL;
        """)

    all_data_quality_count_checks = [
        songplays_check_count, users_check_count,
        songs_check_count, artists_check_count, time_check_count
    ]

    all_data_quality_null_checks = [
        songplays_check_nulls, users_check_nulls, songs_check_nulls,
        artists_check_nulls, time_check_nulls
    ]
