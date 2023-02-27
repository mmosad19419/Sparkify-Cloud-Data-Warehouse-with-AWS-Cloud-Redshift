#!/usr/bin/env python

import configparser
# get config parameters
config = configparser.ConfigParser()
config.read_file(open("cluster_config.cfg", encoding="utf-8"))

ARN = config.get("IAM", "ARN").replace('"', "")
LOG_DATA = config.get("S3", "LOG_DATA").replace('"', "")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH").replace('"', "")

SONG_DATA = config.get("S3", "SONG_DATA").replace('"', "")


# DROP TABLse

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"


# CREATE TABLse

staging_events_table_create= ("""
CREATE TABLE staging_events 
(
se_id                   INTEGER IDENTITY(0,1),
artist                  VARCHAR,
auth                    VARCHAR,
first_name              VARCHAR,
gender                  VARCHAR,
item_in_session         INTEGER,
last_name               VARCHAR,
length                  DECIMAL,
level                   VARCHAR,
location                VARCHAR,
method                  VARCHAR,
page                    VARCHAR,
registration            BIGINT,
session_id              INTEGER,
song                    VARCHAR,
status                  INTEGER,
ts                      BIGINT,
user_agent              VARCHAR,
user_id                 INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    ss_id              INTEGER IDENTITY(0,1),
    num_songs          INTEGER,
    artist_id          VARCHAR,
    artist_latitude    NUMERIC,
    artist_longitude   NUMERIC,
    artist_location    VARCHAR,
    artist_name        VARCHAR,
    song_id            VARCHAR,
    title              VARCHAR,
    duration           NUMERIC,
    year               INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    songplay_id         INTEGER IDENTITY(0,1) PRIMARY KEY UNIQUE NOT NULL SORTKEY,
    start_time          BIGINT NOT NULL,
    ssesion_id          VARCHAR (25) NOT NULL,
    user_id             INT NOT NULL,
    song_id             VARCHAR (25),
    artist_id           VARCHAR (25),
    level               VARCHAR (25) NOT NULL DISTKEY,
    location            VARCHAR (50),
    user_agent          VARCHAR (50),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id             INT PRIMARY KEY UNIQUE NOT NULL SORTKEY,
    first_name          VARCHAR (50) NOT NULL,
    last_name           VARCHAR (50) NOT NULL,
    gender              CHAR,
    level               VARCHAR (25) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id             INT PRIMARY KEY UNIQUE NOT NULL SORTKEY,
    title               VARCHAR (50) NOT NULL,
    artist_id           VARCHAR (25) NOT NULL,
    artist_name         VARCHAR (50) NOT NULL,
    year                INT NOT NULL,
    duration            FLOAT8 NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id           VARCHAR (25) PRIMARY KEY UNIQUE NOT NULL,
    name                VARCHAR (50) NOT NULL SORTKEY,
    location            VARCHAR (50),
    latitude            FLOAT8,
    longitude           FLOAT8
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time          timestamp PRIMARY KEY NOT NULL SORTKEY,
    hour                VARCHAR (25) NOT NULL,
    day                 VARCHAR (25) NOT NULL,
    week                VARCHAR (25) NOT NULL,
    month               VARCHAR (25) NOT NULL,
    year                VARCHAR (25) NOT NULL,
    weekday             VARCHAR (25) NOT NULL);
""")             

# STAGING TABLse

staging_events_copy = ("""
COPY staging_events 
FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON AS '{}'
REGION 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
JSON AS 'auto'
COMPUPDATE off
REGION 'us-west-2'
""").format(SONG_DATA, ARN)


# FINAL TABLse

songplay_table_insert = ("""
INSERT INTO songplay(
    start_time,
    sesion_id,
    user_id,
    song_id,
    artist_id,
    level,
    location,
    user_agent)
SELECT(
    se.ts,
    se.session_id,
    se.user_id,
    ss.song_id,
    ss.artist_id,
    se.level,
    se.location,
    se.user_agent)
FROM staging_events se
JOIN staging_songs ss
ON se.song = ss.title
WHERE se.page = 'NextSong' AND se.user_id IS NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users
    (user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT(
    se.user_id,
    se.first_name,
    se.last_name,
    se.gender,
    se.level)
FROM staging_events se
WHERE user_id IS NOT NULL;
ON CONFLICT (user_id)
DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs(
    song_id,
    title,
    artist_id,
    artist_name,
    year,
    duration)
SELECT(
    song_id,
    title,
    artist_id,
    artist_name,
    year,
    duration)
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT(
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude)
FROM staging_songs
WHERE artist_id IS NOT NULL
ON CONFLICT (artist_id)
DO UPDATE SEt name = excluded.name;
""")

time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT(
(timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time),
DATE_PART(hrs, start_time) AS hour,
DATE_PART(dayofyear, start_time) AS day,
DATE_PART(w, start_time) AS week,
DATE_PART(mons ,start_time) AS month,
DATE_PART(yrs , start_time) AS year,
DATE_PART(dow, start_time) AS weekday
)
FROM staging_events_table;
""")


# QUERY LISTS

# CREATE SCHEMA
create_schema = "CREATE SCHEMA IF NOT EXISTS sparkify"

direct_queries = "SET search_path TO sparkify;"

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create,  
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create,
                        songplay_table_create]


drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]


copy_table_queries = [staging_events_copy,
                      staging_songs_copy]


insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
