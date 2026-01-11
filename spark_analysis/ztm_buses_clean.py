import argparse
import re
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession, functions as F, Window

def default_dt_utc_yesterday() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")

def require_dt(dt: str) -> str:
    if not re.match(r"^\d{8}$", dt):
        raise ValueError(f"--dt must be YYYYMMDD, got: {dt}")
    return dt

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dt", default=default_dt_utc_yesterday(), help="Partition date YYYYMMDD (default: yesterday UTC)")
    p.add_argument("--source_table", default="ztm_buses_raw")
    p.add_argument("--target_table", default="ztm_buses_cleaned")
    p.add_argument("--target_path", default="/user/bigdata/data/parquet/ztm_buses_cleaned")
    args = p.parse_args()

    dt = require_dt(args.dt)

    spark = (
        SparkSession.builder
        .appName(f"ztm_buses_cleaned_{dt}")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    raw = spark.table(args.source_table).where(
        (F.col("dt") == dt) &
        F.col("dt").isNotNull() &
        F.col("hr").isNotNull()
    )

    df = (
        raw
        .withColumn("lat", F.col("Lat"))
        .withColumn("lon", F.col("Lon"))
        .withColumn("vehicle_number", F.col("VehicleNumber"))
        .withColumn("line", F.col("Lines"))
        .withColumn("brigade", F.col("Brigade"))
        .withColumn("time_ms", F.col("Time"))
        .withColumn("ingest_ms", F.col("ingest_time"))
    )

    df = (
        df
        .withColumn("event_ts", F.to_timestamp(F.from_unixtime((F.col("time_ms") / F.lit(1000)).cast("long"))))
        .withColumn("dt", F.date_format(F.col("event_ts"), "yyyyMMdd"))
        .withColumn("hr", F.lpad(F.hour(F.col("event_ts")).cast("string"), 2, "0"))
    )

    df = (
        df
        .filter(F.col("vehicle_number").isNotNull() & (F.length(F.col("vehicle_number")) > 0))
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .filter(F.col("lat").between(-90.0, 90.0))
        .filter(F.col("lon").between(-180.0, 180.0))
        .filter(F.col("event_ts").isNotNull())
    )

    w = Window.partitionBy("vehicle_number", "event_ts").orderBy(F.col("ingest_ms").desc_nulls_last())
    df = (
        df.withColumn("rn", F.row_number().over(w))
          .filter(F.col("rn") == 1)
          .drop("rn")
    )

    out = df.select(
        "line",
        "vehicle_number",
        "brigade",
        "lat",
        "lon",
        "event_ts",
        "ingest_ms",
        "dt",
        "hr"
    )

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {args.target_table} (
            line STRING,
            vehicle_number STRING,
            brigade STRING,
            lat DOUBLE,
            lon DOUBLE,
            event_ts TIMESTAMP,
            ingest_ms BIGINT
        )
        PARTITIONED BY (dt STRING, hr STRING)
        STORED AS PARQUET
        LOCATION '{args.target_path}'
    """)

    (
        out
        .repartition(1)
        .write
        .mode("overwrite")
        .format("parquet")
        .partitionBy("dt", "hr")
        .option("compression", "snappy")
        .save(args.target_path)
    )
    
    hrs = [row.hr for row in out.select("hr").distinct().collect()]
    for hr in hrs:
        spark.sql(f"""
            ALTER TABLE {args.target_table}
            ADD IF NOT EXISTS PARTITION (dt='{dt}', hr='{hr}')
            LOCATION '{args.target_path}/dt={dt}/hr={hr}'
        """)

    spark.stop()


if __name__ == "__main__":
    main()
