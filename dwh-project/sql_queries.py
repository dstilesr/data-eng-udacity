import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create = ("""
create table if not exists staging_events (
    artist text,
    auth varchar(128),
    firstName varchar(128),
    gender varchar(4),
    itemInSession integer,
    lastName varchar(128),
    length real,
    level varchar(32),
    location text,
    method varchar(32),
    page varchar(64),
    registration varchar(128),
    sessionId integer,
    song text,
    status smallint,
    ts bigint sortkey,
    userAgent varchar(256),
    userId integer
);
""")

staging_songs_table_create = ("""
create table if not exists staging_songs (
    song_id varchar(64),
    title text,
    artist_id varchar(64),
    year smallint,
    duration real,
    artist_name text,
    artist_location text,
    artist_latitude double precision,
    artist_longitude double precision
);
""")

songplay_table_create = ("""
create table if not exists songplays (
    songplay_id integer identity(0, 1) primary key,
    start_time timestamp not null references time sortkey distkey,
    user_id integer not null references users,
    level varchar(32),
    song_id varchar(64) references songs,
    artist_id varchar(64) references artists,
    session_id integer,
    location varchar(256),
    user_agent varchar(256)
);
""")

user_table_create = ("""
create table if not exists users (
    user_id integer not null primary key,
    first_name varchar(128),
    last_name varchar(128),
    gender varchar(4),
    level varchar(32)
);
""")

song_table_create = ("""
create table if not exists songs (
    song_id varchar(64) unique not null primary key,
    title text,
    artist_id varchar(64) not null references artists,
    year smallint,
    duration real
);
""")

artist_table_create = ("""
create table if not exists artists (
    artist_id varchar(64) unique not null primary key,
    name text,
    location text,
    latitude double precision,
    longitude double precision
);
""")

time_table_create = ("""
create table if not exists time (
    start_time timestamp unique not null primary key sortkey distkey,
    hour smallint,
    day smallint,
    week smallint,
    month smallint,
    year smallint,
    weekday smallint
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {logs_path}
credentials 'aws_iam_role={role_arn}'
json {json_paths}
region 'us-west-2';
""").format(
    logs_path=config["S3"]["LOG_DATA"],
    json_paths=config["S3"]["LOG_JSONPATH"],
    role_arn=config["IAM"]["ARN"]
)

staging_songs_copy = ("""
copy staging_songs from {songs_path}
credentials 'aws_iam_role={role_arn}'
json 'auto'
region 'us-west-2';
""").format(
    songs_path=config["S3"]["SONG_DATA"],
    role_arn=config["IAM"]["ARN"]
)

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select timestamp 'epoch' +  e.ts/1000 * interval '1 Second' as start_time,
       e.userId as user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId as session_id,
       e.location,
       e.userAgent as user_agent
from staging_events as e
left join staging_songs as s on e.artist = s.artist_name and e.song = s.title
where e.page = 'NextSong';
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct userid as user_id,
       e.firstname as first_name,
       e.lastname as last_name,
       e.gender,
       last_value(e.level)  -- Use this to take only the latest level per user
           over (partition by e.userid
               order by e.ts
               rows between unbounded preceding and unbounded following) as level
from staging_events as e
where e.userid is not null
  and e.userid not in (select distinct user_id from users);
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select distinct s.song_id,
       s.title,
       s.artist_id,
       case when s.year = 0 then null else s.year end as year,
       s.duration
from staging_songs as s
where s.song_id is not null;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
select distinct s.artist_id, -- Use first_value to ensure no duplicates by artist id
       first_value(s.artist_name)
           over (partition by s.artist_id 
               order by year desc 
               rows between unbounded preceding and unbounded following) as name,
       first_value(s.artist_location)
           over (partition by s.artist_id 
               order by year desc 
               rows between unbounded preceding and unbounded following) as location,
       first_value(s.artist_latitude)
           over (partition by s.artist_id 
               order by year desc 
               rows between unbounded preceding and unbounded following) as latitude,
       first_value(s.artist_longitude)
           over (partition by s.artist_id 
               order by year desc 
               rows between unbounded preceding and unbounded following) as longitude
from staging_songs as s
where s.artist_id is not null;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select times_temp.start_time,
       extract(hour from start_time) as hour,
       extract(day from start_time) as day,
       extract(week from start_time) as week,
       extract(month from start_time) as month,
       extract(year from start_time) as year,
       extract(dayofweek from start_time) as weekday
from ( -- subquery to make extraction of year, day, etc less verbose
  select distinct(timestamp 'epoch' +  e.ts/1000 * interval '1 Second') as start_time
  from staging_events as e
  ) as times_temp;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [
    user_table_insert,
    artist_table_insert,
    song_table_insert,
    time_table_insert,
    songplay_table_insert
]
