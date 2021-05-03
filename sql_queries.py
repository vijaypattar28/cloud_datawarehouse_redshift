import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
IAM_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay"
user_table_drop = "DROP TABLE IF EXISTS dim_user"
song_table_drop = "DROP TABLE IF EXISTS dim_song"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
        (
          artist          VARCHAR
        , auth            VARCHAR 
        , firstName       VARCHAR
        , gender          VARCHAR   
        , itemInSession   INTEGER
        , lastName        VARCHAR
        , length          FLOAT
        , level           VARCHAR 
        , location        VARCHAR
        , method          VARCHAR
        , page            VARCHAR
        , registration    BIGINT
        , sessionId       INTEGER
        , song            VARCHAR
        , status          INTEGER
        , ts              TIMESTAMP
        , userAgent       VARCHAR
        , userId          INTEGER
        );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
          num_songs        INTEGER
        , artist_id        VARCHAR
        , artist_latitude  DECIMAL
        , artist_longitude DECIMAL
        , artist_location  VARCHAR
        , artist_name      VARCHAR
        , song_id          VARCHAR
        , title            VARCHAR
        , duration         DECIMAL
        , year             INTEGER
    
    );
    
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_songplay
    (
          songplay_id        INTEGER IDENTITY(0,1) PRIMARY KEY sortkey
        , start_time         TIMESTAMP  NOT NULL
        , user_id            INTEGER    NOT NULL
        , level              VARCHAR
        , song_id            VARCHAR
        , artist_id          VARCHAR
        , session_id         INTEGER    NOT NULL
        , location           VARCHAR
        , user_agent         VARCHAR
    );
    
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_user
    (
            user_id         VARCHAR      PRIMARY KEY
          , first_name      VARCHAR
          , last_name       VARCHAR
          , gender          VARCHAR(1)
          , level           VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_song
    (
            song_id         VARCHAR      PRIMARY KEY
          , title           VARCHAR
          , artist_id       VARCHAR      NOT NULL
          , year            INT
          , duration        FLOAT
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_artist
    (
            artist_id       VARCHAR      PRIMARY KEY
          , name            VARCHAR
          , location        VARCHAR
          , lattitude       FLOAT
          , longitude       FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_time
    (
            start_time      TIMESTAMP      PRIMARY KEY
          , hour            INT
          , day             INT
          , week            INT
          , month           INT
          , year            INT
          , weekday         VARCHAR
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    iam_role {}
    region 'us-west-2'
    FORMAT AS JSON {}
    TIMEFORMAT AS 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(LOG_DATA, IAM_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    iam_role {}
    region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO fact_songplay
    (
          songplay_id
        , start_time
        , user_id
        , level
        , song_id
        , artist_id
        , session_id
        , location
        , user_agent
    )
    SELECT DISTINCT
          se.ts
        , se.user_id
        , se.level
        , ss.song_id
        , ss.artist_id
        , se.session_id
        , se.location
        , se.user_agent
FROM staging_events se
INNER JOIN staging_songs ss ON se.song = ss.title
    AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dim_user
    (
            user_id
          , first_name
          , last_name
          , gender
          , level
    )
    SELECT DISTINCT
          se.user_id
        , se.first_name
        , se.last_name
        , se.gender
        , se.level
FROM staging_events se
WHERE user_id IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO dim_song
    (
            song_id
          , title
          , artist_id
          , year
          , duration
    )
    SELECT DISTINCT
          ss.song_id
        , ss.title
        , ss.artist_id
        , ss.year
        , ss.duration
    FROM staging_songs ss
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO dim_artist
    (
            artist_id
          , name
          , location
          , lattitude
          , longitude
    )
    SELECT DISTINCT
          ss.artist_id, 
        , ss.artist_name
        , ss.artist_location
        , ss.artist_latitude
        , ss.artist_longitude
    FROM staging_songs ss
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO dim_time
    (
            start_time
          , hour
          , day
          , week
          , month
          , year
          , weekday
    )
    SELECT DISTINCT
        , se.ts AS start_time,
        , EXTRACT(HOUR FROM start_time)    as hour
        , EXTRACT(DAY FROM start_time)     as day
        , EXTRACT(WEEKS FROM start_time)   as week
        , EXTRACT(MONTH FROM start_time)   as month
        , EXTRACT(YEAR FROM start_time)    as year
        , to_char(start_time, 'Day')       as weekday
FROM staging_events se WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
