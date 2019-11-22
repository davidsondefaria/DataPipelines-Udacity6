class SqlQueries:
    songplay_table_insert = ("""
        SELECT DISTINCT
                md5(ev.sessionid || ev.start_time) songplay_id,
                ev.start_time, 
                ev.userid, 
                ev.level, 
                sg.songid, 
                sg.artistid, 
                ev.sessionid, 
                ev.location, 
                ev.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
              FROM staging_events
             ) AS ev
        INNER JOIN (SELECT song_id AS songid, artist_id AS artistid, *
              FROM staging_songs
             ) AS sg
        ON ev.song = sg.title
        AND ev.artist = sg.artist_name
        AND ev.length = sg.duration
        WHERE page='NextSong'
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