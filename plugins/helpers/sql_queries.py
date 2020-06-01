class SqlQueries:
    songplay_table_insert = ('''INSERT INTO songplays
        (play_id, start_time, user_id, level, song_id, artist_id, session_id,
        location, user_agent)
        (SELECT
            md5(events.session_id || events.start_time) songplay_id,
            events.start_time,
            events.user_id,
            events.level,
            songs.song_id,
            songs.artist_id,
            events.session_id,
            events.location,
            events.useragent
        FROM (
            SELECT TIMESTAMP 'epoch' + ts/1000 * interval
                '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
        LEFT JOIN staging_songs songs ON
            events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
        WHERE songplay_id IS NOT NULL)
    ''')

    user_table_insert = ('''INSERT INTO users
        (SELECT distinct user_id, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
        AND user_id IS NOT NULL)
    ''')

    song_table_insert = ('''INSERT INTO songs
        (SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs)
    ''')

    artist_table_insert = ('''INSERT INTO artists
        (SELECT distinct artist_id, artist_name,
            artist_location, artist_latitude, artist_longitude
        FROM staging_songs)
    ''')

    time_table_insert = ('''INSERT INTO time
        (start_time, hour, day, week, month, year, weekday)
        (SELECT
            start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dayofweek from start_time)
        FROM songplays)
    ''')
