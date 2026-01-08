DROP TABLE IF EXISTS air_quality;



CREATE EXTERNAL TABLE air_quality (

    location_id BIGINT,

    location_name STRING,

    latitude DOUBLE,

    longitude DOUBLE,

    parameter STRING,

    datetimelocal STRING,

    value DOUBLE

)

PARTITIONED BY (`date` STRING)

STORED AS PARQUET

LOCATION '/user/bigdata/data/parquet/open_aq';



MSCK REPAIR TABLE air_quality;
