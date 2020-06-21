import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config["IAM_ROLE"]["ARN"]
LOG_DATA = config["S3"]["LOG_DATA"]
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config["S3"]["SONG_DATA"]

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist VARCHAR,
        auth VARCHAR,
        first_name VARCHAR,
        gender VARCHAR(1),
        item_in_session SMALLINT,
        last_name VARCHAR,
        length FLOAT,
        level VARCHAR(4),
        location VARCHAR,
        method VARCHAR(6),
        page VARCHAR,
        registration FLOAT,
        session_id SMALLINT,
        song VARCHAR,
        status SMALLINT,
        ts BIGINT,
        user_agent VARCHAR,
        user_id INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude VARCHAR,
        artist_longitude VARCHAR,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration DECIMAL,
        year SMALLINT
    )
    diststyle even;
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay
    (
        songplay_id INTEGER IDENTITY(0,1) sortkey,
        start_time TIMESTAMP,
        user_id INTEGER NOT NULL,
        level VARCHAR(4),
        song_id VARCHAR distkey,
        artist_id VARCHAR,
        session_id SMALLINT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id INTEGER NOT NULL sortkey,
        first_name VARCHAR NOT NULL,
        last_name VARCHAR NOT NULL,
        gender VARCHAR(1) NOT NULL,
        level VARCHAR(4) NOT NULL
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song
    (
        song_id VARCHAR NOT NULL distkey sortkey,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year SMALLINT,
        duration DECIMAL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist
    (
        artist_id VARCHAR NOT NULL sortkey,
        name VARCHAR NOT NULL,
        location VARCHAR,
        lattitude DECIMAL,
        longitude DECIMAL
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time TIMESTAMP NOT NULL sortkey,
        hour SMALLINT,
        day SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
  COPY staging_events
  FROM '{}'
  CREDENTIALS 'aws_iam_role={}'
  REGION 'us-west-2'
  FORMAT AS JSON '{}';
""").format(LOG_DATA, ARN, LOG_JSONPATH )

staging_songs_copy = ("""
   COPY staging_songs
   FROM '{}'
   CREDENTIALS 'aws_iam_role={}'
   REGION 'us-west-2'
   JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO
    songplay (start_time, user_id, level, song_id,
    artist_id, session_id, location, user_agent)
    SELECT (TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 Second ') AS start_time, 
    se.user_id, se.level, ss.song_id,
    ss.artist_id, se.session_id, se.location, se.user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title
    WHERE se.page ='NextSong' AND se.user_id IS NOT NULL AND
    ss.song_id IS NOT NULL AND ss.artist_id IS NOT NULL
    AND se.artist = ss.artist_name
""")

user_table_insert = ("""
    INSERT INTO
    users(user_id, first_name, last_name, gender, level)
    SELECT se.user_id, se.first_name, se.last_name, se.gender, se.level
    FROM staging_events se
    WHERE se.user_id is not null and se.user_id NOT IN (SELECT DISTINCT user_id FROM users)
    ORDER BY se.ts

""")

song_table_insert = ("""
    INSERT INTO
    song(song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs ss
    WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO
    artist(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location,
    ss.artist_latitude, ss.artist_longitude
    FROM staging_songs ss
    WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO
    time(start_time, hour, day, month, year, weekday)
    SELECT
        start_time,
        EXTRACT(h from start_time) As hour,
        EXTRACT(d from start_time) As day,
        EXTRACT(mon from start_time) As month,
        EXTRACT(y from start_time) As year,
        EXTRACT(dw from start_time) As weekday
    FROM(
        SELECT DISTINCT
         TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ' AS start_time
        FROM staging_events
        ORDER BY start_time
        )
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
    songplay_table_create, user_table_create,
    song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
    songplay_table_drop, user_table_drop, song_table_drop,
    artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
    song_table_insert, artist_table_insert, time_table_insert]
