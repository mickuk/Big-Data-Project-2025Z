DROP TABLE IF EXISTS ztm_buses;

CREATE EXTERNAL TABLE ztm_buses (
    lines STRING,
    lon DOUBLE,
    vehicleNumber STRING,
    `time` STRING,
    lat DOUBLE,
    brigade STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/bigdata/data/csv/ztm'
TBLPROPERTIES ("skip.header.line.count"="1");
