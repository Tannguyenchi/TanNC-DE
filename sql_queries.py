import configparser
from pickle import NONE


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events(
                    artist         VARCHAR,
                    auth             VARCHAR,
                    firstName        VARCHAR,
                    gender           VARCHAR,
                    ItemInSession        INT,
                    lastName         VARCHAR,
                    length             FLOAT,
                    level            VARCHAR,
                    location         VARCHAR,
                    method           VARCHAR,
                    page             VARCHAR,
                    registration     VARCHAR,
                    sessionId            INT,
                    song             VARCHAR,
                    status               INT,
                    ts                  INT8,
                    userAgent        VARCHAR,
                    userId               INT)
""")

staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs(
                    song_id      VARCHAR PRIMARY KEY,
                    artist_id                   VARCHAR,
                    artist_latitude               FLOAT,
                    artist_longitude              FLOAT,
                    artist_location             VARCHAR,
                    artist_name                 VARCHAR,
                    duration                      FLOAT,
                    num_songs                       INT,
                    title                       VARCHAR,
                    year                            INT)
""")

songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays(
                    songplay_id               INT IDENTITY(0,1) PRIMARY KEY,
                    start_time    TIMESTAMP WITHOUT TIME ZONE sortkey distkey,
                    user_id                                               INT,
                    level                                             VARCHAR,
                    song_id                                           VARCHAR,
                    artist_id                                         VARCHAR,
                    session_id                                            INT,
                    location                                          VARCHAR,
                    user_agent                                        VARCHAR)
""")

user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users(
                    user_id       INT PRIMARY KEY,
                    first_name              VARCHAR,
                    last_name               VARCHAR,
                    gender                  VARCHAR,
                    level                   VARCHAR)
""")

song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs(
                    song_id       VARCHAR PRIMARY KEY,
                    title                       VARCHAR,
                    artist_id                   VARCHAR,
                    year                            INT,
                    duration                      FLOAT)
""")

artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists(
                    artist_id         VARCHAR PRIMARY KEY,
                    artist_name                            VARCHAR,
                    location                        VARCHAR,
                    latitude                        FLOAT,
                    longitude                       FLOAT)
""")

time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY distkey sortkey,
                    hour INT,
                    day INT,
                    week INT,
                    month INT,
                    year INT,
                    weekday VARCHAR)
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {}
                        credentials 'aws_iam_role={}'
                        region      'us-west-2'
                        format       as JSON {}
                        timeformat   as 'epochmillisecs'
""").format(
    config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE', 'ARN'),
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region      'us-west-2'
    format       as JSON 'auto'
""").format(
    config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,
                                                user_id,
                                                level,
                                                song_id,
                                                artist_id,
                                                session_id,
                                                location,
                                                user_agent)
SELECT
DISTINCT
TIMESTAMP 'epoch' + (events.ts / 1000) * INTERVAL '1 second' AS start_time,
            events.userId AS    user_id,
                                events.level AS      level,
                                songs.song_id AS    song_id,
                                songs.artist_id AS  artist_id,
                                events.sessionId AS session_id,
                                events.location AS   location,
                                events.userAgent AS  user_agent
                            FROM staging_events events
                            JOIN staging_songs  songs
ON events.song = songs.title
AND events.artist = songs.artist_name
AND events.page = 'NextSong'
AND events.length = songs.duration
""")

user_table_insert = ("""INSERT INTO users(user_id,
                                        first_name,
                                        last_name,
                                        gender,
                                        level)
                        SELECT DISTINCT(events.userId) AS       user_id,
                            events.firstName           AS    first_name,
                            events.lastName            AS     last_name,
                            events.gender              AS        gender,
                            events.level               AS          level
                        FROM staging_events events
                        WHERE user_id IS NOT NULL AND page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs(song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration)
                        SELECT DISTINCT(songs.song_id) AS       song_id,
                            songs.title                AS         title,
                            songs.artist_id            AS     artist_id,
                            songs.year                 AS          year,
                            songs.duration             AS       duration
                        FROM staging_songs songs
""")

artist_table_insert = ("""INSERT INTO artists(artist_id,
                                            artist_name,
                                            location,
                                            latitude,
                                            longitude)
                        SELECT DISTINCT(songs.artist_id) AS       artist_id,
                            songs.artist_name            AS     artist_name,
                            songs.artist_location        AS location,
                            songs.artist_latitude        AS latitude,
                            songs.artist_longitude       AS longitude
                        FROM staging_songs songs
""")

time_table_insert = ("""INSERT INTO time(start_time,
                                        hour,
                                        day,
                                        week,
                                        month,
                                        year,
                                        weekDay)
                        SELECT  start_time,
                                EXTRACT(HOUR FROM start_time),
                                EXTRACT(DAY FROM start_time),
                                EXTRACT(WEEK FROM start_time),
                                EXTRACT(MONTH FROM start_time),
                                EXTRACT(YEAR FROM start_time),
                                EXTRACT(DAYOFWEEK FROM start_time)
                        FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
