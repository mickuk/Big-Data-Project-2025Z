CREATE EXTERNAL TABLE IF NOT EXISTS weather_data (
  event_time     BIGINT,
  weather_id     INT,
  weather_main   STRING,
  weather_desc   STRING,
  temp           DOUBLE,
  feels_like     DOUBLE,
  pressure       INT,
  humidity       INT,
  wind_speed     DOUBLE,
  rain_1h        DOUBLE,
  snow_1h        DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/bigdata/data/parquet/weather_data';