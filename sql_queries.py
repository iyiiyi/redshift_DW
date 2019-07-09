import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays cascade"
user_table_drop           = "DROP TABLE IF EXISTS users"
song_table_drop           = "DROP TABLE IF EXISTS songs"
artist_table_drop         = "DROP TABLE IF EXISTS artists"
time_table_drop           = "DROP TABLE IF EXISTS time"

# CREATE TABLES

## staging_events table contains a time series log data.
staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events (
    artist              varchar(256),
    auth                varchar(256)      NOT NULL,
    firstName           varchar(256),
    gender              varchar(4),
    itemInSession       varchar(256)      NOT NULL,
    lastName            varchar(256),
    length              double precision,
    level               varchar(256)      NOT NULL,
    location            varchar(256),
    method              varchar(256)      NOT NULL,
    page                varchar(256)      NOT NULL,
    registration        double precision,
    sessionId           integer           NOT NULL,
    song                varchar(256),
    status              varchar(256)      NOT NULL,
    ts                  bigint            NOT NULL    sortkey,
    userAgent           varchar(256),
    userId              integer
  );
""")

## staging_songs table contains song master data
staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           integer           NOT NULL,
    artist_id           varchar(256)      NOT NULL,
    artist_latitude     varchar(256),
    artist_longitude    varchar(256),
    artist_location     varchar(256), 
    artist_name         varchar(256)      NOT NULL, 
    song_id             varchar(256)      NOT NULL,
    title               varchar(256)      NOT NULL, 
    duration            double precision  NOT NULL,
    year                integer           NOT NULL
  );
""")

# songplays table is a fact table which can be joined with 4 other dimension tables.
## Make key values which can be joined with dimension tables be a part of sortkey: start_time, user_id, song_id, artist_id
songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id         bigint            IDENTITY(0,1), 
    start_time          bigint            NOT NULL    distkey, 
    user_id             integer           NOT NULL, 
    level               varchar(256)      NOT NULL, 
    song_id             varchar(256)      NOT NULL, 
    artist_id           varchar(256)      NOT NULL, 
    session_id          integer           NOT NULL, 
    location            varchar(256), 
    user_agent          varchar(256)      NOT NULL
  ) SORTKEY (start_time, user_id, song_id, artist_id);
""")

## users table is a dimension table about service users
user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id             integer           UNIQUE NOT NULL sortkey,
    first_name          varchar(256)      NOT NULL,
    last_name           varchar(256)      NOT NULL,
    gender              varchar(4)        NOT NULL,
    level               varchar(256)      NOT NULL
  )
  diststyle all;
""")

## songs table is a dimension table about songs
song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
    song_id             varchar(256)      UNIQUE NOT NULL sortkey,
    title               varchar(256)      NOT NULL,
    artist_id           varchar(256)      NOT NULL,
    year                integer           NOT NULL,
    duration            double precision  NOT NULL
  )
  diststyle all;
""")

## artists table is a dimension table about artists
artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id           varchar(256)      UNIQUE NOT NULL,
    name                varchar(256)      NOT NULL,
    location            varchar(256),
    lattitude           varchar(25),
    longitude           varchar(25)
  )
  diststyle all;
""")

## time table is a dimension table about start_time (music play start time)
time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time (
    start_time          bigint           UNIQUE NOT NULL sortkey distkey,
    hour                varchar(2)       NOT NULL,
    day                 varchar(2)       NOT NULL,
    week                varchar(2)       NOT NULL,
    month               varchar(2)       NOT NULL,
    year                varchar(4)       NOT NULL,
    weekday             varchar(3)       NOT NULL
  );
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
json {}
region 'us-west-2'
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
json 'auto'
region 'us-west-2'
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

## For an event in staging_events with page='NextSong' value and matching song title and artist name, insert a record into songplays
songplay_table_insert = ("""
  INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
  SELECT staging_events.ts, staging_events.userId, staging_events.level, staging_songs.song_id,
         staging_songs.artist_id, staging_events.sessionId, staging_events.location, staging_events.userAgent
  FROM staging_events, staging_songs
  WHERE staging_events.page = 'NextSong'
    AND staging_events.song = staging_songs.title
    AND staging_events.artist = staging_songs.artist_name;
""")

## users table must have unique user_id values which would be primary key
user_table_insert = ("""
  INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT userId, max(firstName), max(lastName), max(gender), max(level)
  FROM staging_events
  WHERE userId IS NOT NULL
  GROUP BY userId;
""")

## songs table must have unique song_id values which would be primary key
song_table_insert = ("""
  INSERT INTO songs (song_id, title, artist_id, year, duration)
  SELECT song_id, title, artist_id, year, duration
  FROM staging_songs
  WHERE song_id IS NOT NULL;
""")

## artists table must have unique artist_id values which would be primary key
## Since stagins_songs can have multiple songs by the same artist, get max values of fields except artist_id.
artist_table_insert = ("""
  INSERT INTO artists (artist_id, name, location, lattitude, longitude)
  SELECT artist_id, max(artist_name), max(artist_location), max(artist_latitude), max(artist_longitude)
  FROM staging_songs
  WHERE artist_id IS NOT NULL
  GROUP BY artist_id;
""")

## time table must have unique start_Time values which would be primary key
##   hour : hour of a day
##   day  : day of a month
##   week : week of a year
##   month : month of a year
##   year : year
##   weekday : abbreviated day name like MON, TUE, ...
time_table_insert = ("""
  INSERT INTO time (start_time, hour, day, week, month, year, weekday)
  SELECT ts, 
         to_char((timestamp 'epoch' + ts/1000 * interval '1 second'), 'HH24') as hour,
         to_char((timestamp 'epoch' + ts/1000 * interval '1 second'), 'DD') as day,
         to_char((timestamp 'epoch' + ts/1000 * interval '1 second'), 'WW') as week,
         to_char((timestamp 'epoch' + ts/1000 * interval '1 second'), 'MM') as month,
         to_char((timestamp 'epoch' + ts/1000 * interval '1 second'), 'YYYY') as year,
         to_char((timestamp 'epoch' + ts/1000 * interval '1 second'), 'DY') as weekday
  FROM staging_events
  GROUP BY ts;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
