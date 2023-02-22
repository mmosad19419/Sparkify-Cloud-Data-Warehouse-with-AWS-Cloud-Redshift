import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('cluster_config.cfg')


# CREATE SCHEMA
create_schema = "CREATE SCHEMA IF NOT EXISTS sparkify"

direct_queries = "SET search_path TO sparkify;"


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
""")

staging_songs_table_create = ("""
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id     IDENTIFY (0, 1) PRIMARY KEY UNIQUE NOT NULL SORT KEY,
    start_time      BIGINT NOT NULL,
    session_id      VARCHAR (25) NOT NULL,
    user_id         INT NOT NULL,
    song_id         VARCHAR (25),
    artist_id       VARCHAR (25),
    level           VARCHAR (25) NOT NULL DIST KEY,
    location        VARCHAR (50),
    user_agent      VARCHAR (50),
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (song_id) REFERENCES songs (song_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id),
    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
);
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id         INT PRIMARY KEY UNIQUE NOT NULL SORT KEY,
    first_name      VARCHAR (50) NOT NULL,
    last_name       VARCHAR (50) NOT NULL,
    gender          CHAR,
    level           VARCHAR (25) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id         INT PRIMARY KEY UNIQUE NOT NULL SORT KEY,
    title           VARCHAR (50) NOT NULL,
    artist_id       VARCHAR (25) NOT NULL,
    artist_name     VARCHAR (50) NOT NULL,
    year            INT NOT NULL,
    duration        FLOAT8 NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id       VARCHAR (25) PRIMARY KEY UNIQUE NOT NULL,
    name            VARCHAR (50) NOT NULL SORT KEY,
    location        VARCHAR (50),
    latitude        FLOAT8,
    longitude       FLOAT8
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time      BIGINT PRIMARY KEY NOT NULL SORT KEY,
    hour            VARCHAR (25) NOT NULL,
    day             VARCHAR (25) NOT NULL,
    week            VARCHAR (25) NOT NULL,
    month           VARCHAR (25) NOT NULL,
    year            VARCHAR (25) NOT NULL,
    weekday         VARCHAR (25) NOT NULL);
""")
                     

# STAGING TABLES

staging_events_copy = ("""
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
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


copy_table_queries = [staging_events_copy,
                      staging_songs_copy]


insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]
