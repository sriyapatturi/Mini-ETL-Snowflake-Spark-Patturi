
CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.SPATTURI_STAGE
    STORAGE_INTEGRATION = s3_int
    URL='s3://techcatalyst-public/dw_stage/Sriya';
    
LIST @TECHCATALYST_DE.EXTERNAL_STAGE.SPATTURI_STAGE;
LIST @SPATTURI_STAGE PATTERN='.*parquet.*';

CREATE OR REPLACE FILE FORMAT SPATTURI_parquet_format
TYPE = 'PARQUET';


create TRANSIENT TABLE TECHCATALYST_DE.SPATTURI.SONGS_DIM(
    song_id string, 
    title string,
    artist_id string,
    year integer,
    duration double
);

COPY INTO TECHCATALYST_DE.SPATTURI.SONGS_DIM
FROM @SPATTURI_STAGE/songs_table/
FILE_FORMAT = 'spatturi_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * from TECHCATALYST_DE.SPATTURI.SONGS_DIM;

create TRANSIENT TABLE TECHCATALYST_DE.SPATTURI.ARTIST_DIM(
    artist_id string,
    artist_name string,
    artist_location string,
    artist_latitude double,
    artist_longitude double
);

COPY INTO TECHCATALYST_DE.SPATTURI.ARTIST_DIM
FROM @SPATTURI_STAGE/artists_table/
FILE_FORMAT = 'spatturi_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * FROM TECHCATALYST_DE.SPATTURI.ARTIST_DIM;

create TRANSIENT TABLE TECHCATALYST_DE.SPATTURI.TIME_DIM (
    ts bigint,
    datetime string,
    start_time string,
    year integer,
    month integer,
    dayofmonth integer,
    weekofyear integer
);

COPY INTO TECHCATALYST_DE.SPATTURI.TIME_DIM
FROM @SPATTURI_STAGE/time_table/
FILE_FORMAT = 'spatturi_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


SELECT * from TECHCATALYST_DE.SPATTURI.TIME_DIM;

create TRANSIENT TABLE TECHCATALYST_DE.SPATTURI.USER_DIM (
    userId string,
    firstName string,
    lastName string,
    gender string,
    level string
);

COPY INTO TECHCATALYST_DE.SPATTURI.USER_DIM
FROM @SPATTURI_STAGE/user_table/
FILE_FORMAT = 'spatturi_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * FROM TECHCATALYST_DE.SPATTURI.USER_DIM;

create TRANSIENT TABLE TECHCATALYST_DE.SPATTURI.SONGPLAYS_FACT (
    songplay_id integer,
    ts bigint,
    userId string,
    level string,
    song_id string,
    artist_id string,
    sessionId bigint,
    location string,
    userAgent string
);

COPY INTO TECHCATALYST_DE.SPATTURI.SONGPLAYS_FACT
FROM @SPATTURI_STAGE/songplays_table/
FILE_FORMAT = 'spatturi_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

SELECT * from TECHCATALYST_DE.SPATTURI.SONGPLAYS_FACT;


-- Give me the names of songs that were played in Waterloo-Cedar Falls, IA
SELECT TITLE
FROM TECHCATALYST_DE.SPATTURI.SONGS_DIM
JOIN TECHCATALYST_DE.SPATTURI.SONGPLAYS_FACT ON TECHCATALYST_DE.SPATTURI.SONGS_DIM.SONG_ID = TECHCATALYST_DE.SPATTURI.SONGPLAYS_FACT.SONG_ID
WHERE TECHCATALYST_DE.SPATTURI.SONGPLAYS_FACT.LOCATION = 'Waterloo-Cedar Falls, IA';
