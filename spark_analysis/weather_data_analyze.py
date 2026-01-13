import argparse
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, StringType, DoubleType


def main():
    parser = argparse.ArgumentParser(description="Analiza danych pogodowych")
    parser.add_argument("--dt", required=True, help="Data YYYYMMDD")
    parser.add_argument("--source_table", default="weather_data",
                        help="Nazwa tabeli Hive z danymi pogodowymi")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"weather_analysis_{args.dt}")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Wczytanie danych z Hive
    query = f"""
        SELECT * FROM {args.source_table}
        WHERE `date` = '{args.dt}'
    """

    weather_df = spark.sql(query)
    print(f"Załadowano {weather_df.count()} pomiarów pogodowych dla daty {args.dt}")

    # Konwersja event_time (UNIX timestamp w milisekundach) na timestamp
    weather_df = weather_df.withColumn(
        "event_ts",
        F.from_unixtime(F.col("event_time") / 1000)
    ).withColumn(
        "hour",
        F.hour(F.from_unixtime(F.col("event_time") / 1000))
    )

    # Podstawowe statystyki
    print("\n=== PODSTAWOWE STATYSTYKI ===")
    weather_df.select(
        F.count("*").alias("total_records"),
        F.min("temp").alias("min_temp"),
        F.max("temp").alias("max_temp"),
        F.avg("temp").alias("avg_temp"),
        F.avg("humidity").alias("avg_humidity"),
        F.avg("pressure").alias("avg_pressure")
    ).show()

    # Analiza średnich wartości według warunków pogodowych (weather_main)
    weather_stats = weather_df.groupBy("weather_main").agg(
        F.count("*").alias("measurement_count"),
        F.round(F.avg("temp"), 2).alias("avg_temp"),
        F.round(F.avg("feels_like"), 2).alias("avg_feels_like"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
        F.round(F.min("temp"), 2).alias("min_temp"),
        F.round(F.max("temp"), 2).alias("max_temp")
    ).orderBy(F.desc("measurement_count"))

    print("\n=== STATYSTYKI WEDŁUG WARUNKÓW POGODOWYCH ===")
    weather_stats.show(truncate=False)

    # Analiza średnich wartości według godziny
    hourly_stats = weather_df.groupBy("hour").agg(
        F.count("*").alias("measurement_count"),
        F.round(F.avg("temp"), 2).alias("avg_temp"),
        F.round(F.avg("humidity"), 2).alias("avg_humidity"),
        F.round(F.avg("wind_speed"), 2).alias("avg_wind_speed"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure")
    ).orderBy("hour")

    print("\n=== STATYSTYKI WEDŁUG GODZINY ===")
    hourly_stats.show(24, truncate=False)

    # Analiza opadów
    rain_snow_df = weather_df.filter(
        (F.col("rain_1h").isNotNull() & (F.col("rain_1h") > 0)) |
        (F.col("snow_1h").isNotNull() & (F.col("snow_1h") > 0))
    )

    if rain_snow_df.count() > 0:
        print("\n=== STATYSTYKI OPADÓW ===")
        rain_snow_df.select(
            F.count("*").alias("measurements_with_precipitation"),
            F.round(F.avg("rain_1h"), 2).alias("avg_rain_1h"),
            F.round(F.max("rain_1h"), 2).alias("max_rain_1h"),
            F.round(F.avg("snow_1h"), 2).alias("avg_snow_1h"),
            F.round(F.max("snow_1h"), 2).alias("max_snow_1h")
        ).show()
    else:
        print("\n=== BRAK OPADÓW W TYM DNIU ===")

    # Przygotowanie danych do zapisu w HBase
    # Agregacja danych według warunków pogodowych z dodaniem daty
    weather_stats_with_date = weather_stats.withColumn("dt", F.lit(args.dt))

    # Konwersja do listy rekordów
    weather_records = weather_stats_with_date.collect()

    print(f"\nPrzygotowano {len(weather_records)} rekordów do zapisu w HBase")

    # Zapis do HBase
    if weather_records:
        try:
            import happybase
            
            conn = happybase.Connection(host="localhost", port=8000, transport="buffered")
            table_name = "weather_analysis"
            column_families = {'cf': dict()}

            # Utworzenie tabeli jeśli nie istnieje
            if table_name.encode() not in conn.tables():
                conn.create_table(table_name, column_families)
                print(f"Utworzono tabelę HBase: {table_name}")

            table = conn.table(table_name)

            with table.batch(batch_size=1000) as b:
                for record in weather_records:
                    # Klucz: data_warunek_pogodowy
                    rowkey = f"{record['dt']}_{record['weather_main']}"
                    
                    hbase_row = {
                        b"cf:dt": str(record['dt']).encode(),
                        b"cf:weather_main": str(record['weather_main']).encode(),
                        b"cf:measurement_count": str(record['measurement_count']).encode(),
                        b"cf:avg_temp": str(record['avg_temp']).encode(),
                        b"cf:avg_feels_like": str(record['avg_feels_like']).encode(),
                        b"cf:avg_humidity": str(record['avg_humidity']).encode(),
                        b"cf:avg_pressure": str(record['avg_pressure']).encode(),
                        b"cf:avg_wind_speed": str(record['avg_wind_speed']).encode(),
                        b"cf:min_temp": str(record['min_temp']).encode(),
                        b"cf:max_temp": str(record['max_temp']).encode(),
                    }
                    b.put(rowkey.encode(), hbase_row)

            conn.close()
            print(f"\n Zapisano {len(weather_records)} rekordów do HBase (tabela: {table_name}).")

        except ImportError:
            print("\n Moduł happybase nie jest zainstalowany. Pomijam zapis do HBase.")
            print("  Aby zainstalować: pip3 install happybase")
        except Exception as e:
            print(f"\n Błąd podczas zapisu do HBase: {e}")
            print("  Upewnij się, że HBase Thrift Server działa na porcie 8000")
            print("  Uruchom: hbase thrift start -p 8000")

    spark.stop()


if __name__ == "__main__":
    main()


# INSTRUKCJE URUCHOMIENIA:
#
# 1. Uruchom HBase Thrift Server:
#    hbase thrift start -p 8000
#
# 2. Uruchom analizę:
#    spark-submit weather_data_analyze.py --dt 20260113
#
# 3. Sprawdź dane w HBase:
#    scan 'weather_analysis', {LIMIT => 10}
#    get 'weather_analysis', '20260113_Clear'
#
# Zarządzanie Tabelą HBase:
#    disable 'weather_analysis'
#    drop 'weather_analysis'
