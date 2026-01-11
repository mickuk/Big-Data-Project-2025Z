import argparse
import requests
from datetime import datetime
from functools import lru_cache
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, ArrayType
)

API_KEY = "51b9e237-4a51-4cf0-920c-933428d34e39"
BASE_URL = "https://api.um.warszawa.pl/api/action"
TIMETABLE_ENDPOINT = "dbtimetable_get"
TIMETABLE_ID = "e923fa0e-d96c-43f9-ae6e-60518c9f3238"

MAX_DISTANCE_METERS = 700.0
EARTH_RADIUS_KM = 6371.0


@lru_cache(maxsize=10000)
def get_timetable_cached(stop_id: str, stop_nr: str, line: str) -> tuple:
    stop_id_padded = str(stop_id).zfill(4)
    stop_nr_padded = str(stop_nr).zfill(2)

    params = {
        'id': TIMETABLE_ID,
        'busstopId': stop_id_padded,
        'busstopNr': stop_nr_padded,
        'line': str(line),
        'apikey': API_KEY
    }

    try:
        response = requests.get(f'{BASE_URL}/{TIMETABLE_ENDPOINT}', params=params, timeout=10)
        result = response.json().get('result')

        if not result:
            return tuple()

        timetable = []
        for item in result:
            if item and len(item) > 0:
                time_entry = item[-1].get('value', '') if isinstance(item[-1], dict) else ''
                direction = ''
                if len(item) > 1 and isinstance(item[-2], dict):
                    direction = item[-2].get('value', '')
                timetable.append((time_entry, direction))

        return tuple(timetable)

    except Exception as e:
        print(f"  API error for {stop_id_padded}/{stop_nr_padded}/{line}: {e}")
        return tuple()


def calculate_time_diff_minutes(measurement_time_str: str, timetable: tuple) -> tuple:
    """
    Liczy TimeDiffMinutes jak w Twoim drugim projekcie:
    time_diff = ABS(measurement_time - scheduled_time)

    Zwraca:
    (time_diff_minutes, nearest_scheduled_time, nearest_direction)
    """
    if not timetable:
        return (None, None, None)

    try:
        measurement_time = datetime.strptime(measurement_time_str, '%H:%M:%S')
    except ValueError:
        return (None, None, None)

    min_abs_diff = float('inf')
    nearest_time = None
    nearest_direction = None

    for time_str, direction in timetable:
        if not time_str:
            continue

        try:
            parts = time_str.split(':')
            hour = int(parts[0]) % 24  # 24 -> 0
            time_str_fixed = f"{hour:02d}:{parts[1]}:{parts[2]}"

            scheduled = datetime.strptime(time_str_fixed, '%H:%M:%S')

            diff_min = abs((measurement_time - scheduled).total_seconds() / 60)

            if diff_min < min_abs_diff:
                min_abs_diff = diff_min
                nearest_time = time_str_fixed
                nearest_direction = direction

        except (ValueError, IndexError):
            continue

    if nearest_time is None:
        return (None, None, None)

    return (round(min_abs_diff, 2), nearest_time, nearest_direction)


def main():
    parser = argparse.ArgumentParser(description="Analiza odchylenia od rozkładu autobusów ZTM (TimeDiff)")
    parser.add_argument("--dt", required=True, help="Data YYYYMMDD")
    parser.add_argument("--hour", type=int, required=True, help="Godzina 0-23")
    parser.add_argument("--source_table", default="ztm_buses_cleaned",
                        help="Nazwa tabeli Hive z danymi autobusów")
    parser.add_argument("--stops_path", required=True, help="Ścieżka do pliku stops.txt (GTFS)")
    parser.add_argument("--output_path", required=True, help="Ścieżka wyjściowa")
    parser.add_argument("--lines", nargs="*", help="Lista linii do analizy (domyślnie wszystkie)")
    parser.add_argument("--max_distance", type=float, default=MAX_DISTANCE_METERS,
                        help="Max odległość od przystanku w metrach")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName(f"ztm_timediff_analysis_{args.dt}_h{args.hour}")
        .enableHiveSupport()
        .getOrCreate()
    )

    stops_schema = StructType([
        StructField("stop_id", StringType(), True),
        StructField("stop_code", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("stop_lat", DoubleType(), True),
        StructField("stop_lon", DoubleType(), True),
    ])

    stops_df = (
        spark.read
        .option("header", "true")
        .schema(stops_schema)
        .csv(args.stops_path)
    )

    stops_df = (
        stops_df
        .withColumn(
            "zespol",
            F.when(
                F.col("stop_id").contains("_"),
                F.split(F.col("stop_id"), "_").getItem(0)
            ).otherwise(F.col("stop_id"))
        )
        .withColumn(
            "slupek",
            F.when(
                F.col("stop_id").contains("_"),
                F.split(F.col("stop_id"), "_").getItem(1)
            ).otherwise(F.col("stop_code"))
        )
        .withColumn("zespol", F.lpad(F.col("zespol"), 4, "0"))
        .withColumn("slupek", F.lpad(F.col("slupek"), 2, "0"))
        .select(
            F.col("zespol").alias("stop_zespol"),
            F.col("slupek").alias("stop_slupek"),
            F.col("stop_name"),
            F.col("stop_lat"),
            F.col("stop_lon")
        )
    )

    stops_broadcast = F.broadcast(stops_df)
    print(f"Załadowano {stops_df.count()} przystanków")

    hr_str = str(args.hour).zfill(2)

    lines_filter = ""
    if args.lines:
        lines_list = ", ".join(f"'{l}'" for l in args.lines)
        lines_filter = f"AND line IN ({lines_list})"

    query = f"""
        SELECT * FROM {args.source_table}
        WHERE dt = '{args.dt}'
          AND hr = '{hr_str}'
          {lines_filter}
    """

    bus_df = spark.sql(query)

    bus_df = bus_df.withColumn(
        "time_str",
        F.date_format(F.col("event_ts"), "HH:mm:ss")
    )

    print(f"Załadowano {bus_df.count()} pomiarów autobusów")

    # CROSS JOIN z przystankami (broadcast)
    bus_with_stops = bus_df.crossJoin(stops_broadcast)

    # Haversine
    bus_with_stops = bus_with_stops.withColumn(
        "lat1_rad", F.radians(F.col("lat"))
    ).withColumn(
        "lat2_rad", F.radians(F.col("stop_lat"))
    ).withColumn(
        "lon1_rad", F.radians(F.col("lon"))
    ).withColumn(
        "lon2_rad", F.radians(F.col("stop_lon"))
    ).withColumn(
        "dlat", F.col("lat2_rad") - F.col("lat1_rad")
    ).withColumn(
        "dlon", F.col("lon2_rad") - F.col("lon1_rad")
    ).withColumn(
        "a",
        F.pow(F.sin(F.col("dlat") / 2), 2) +
        F.cos(F.col("lat1_rad")) * F.cos(F.col("lat2_rad")) *
        F.pow(F.sin(F.col("dlon") / 2), 2)
    ).withColumn(
        "c", 2 * F.asin(F.sqrt(F.col("a")))
    ).withColumn(
        "distance_m", F.col("c") * EARTH_RADIUS_KM * 1000
    )

    window = Window.partitionBy(
        "line", "vehicle_number", "event_ts"
    ).orderBy(F.col("distance_m").asc())

    nearest_stops = (
        bus_with_stops
        .withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .filter(F.col("distance_m") <= args.max_distance)
        .select(
            "line",
            "vehicle_number",
            "brigade",
            "lat",
            "lon",
            "event_ts",
            "time_str",
            "stop_zespol",
            "stop_slupek",
            "stop_name",
            "distance_m"
        )
    )

    print(f"Znaleziono {nearest_stops.count()} pomiarów w zasięgu przystanków")

    unique_stop_lines = (
        nearest_stops
        .select("stop_zespol", "stop_slupek", "line")
        .distinct()
        .collect()
    )

    print(f"Pobieranie rozkładów dla {len(unique_stop_lines)} kombinacji przystanek-linia...")

    timetables = {}
    for i, row in enumerate(unique_stop_lines[:5]):
        print(f"  DEBUG [{i}]: zespol='{row.stop_zespol}', slupek='{row.stop_slupek}', line='{row.line}'")

    for row in unique_stop_lines:
        key = (row.stop_zespol, row.stop_slupek, row.line)
        timetables[key] = get_timetable_cached(*key)

    print(f"Pobrano {sum(1 for v in timetables.values() if v)} rozkładów")

    bus_data = nearest_stops.collect()

    results = []
    for row in bus_data:
        key = (row.stop_zespol, row.stop_slupek, row.line)
        timetable = timetables.get(key, tuple())

        time_diff_min, scheduled_time, direction = calculate_time_diff_minutes(
            row.time_str, timetable
        )

        if time_diff_min is not None:
            results.append({
                "line": row.line,
                "vehicle_number": row.vehicle_number,
                "brigade": row.brigade,
                "lat": row.lat,
                "lon": row.lon,
                "event_ts": row.event_ts,
                "time_str": row.time_str,
                "stop_zespol": row.stop_zespol,
                "stop_slupek": row.stop_slupek,
                "stop_name": row.stop_name,
                "distance_m": round(row.distance_m, 2),
                "time_diff_minutes": time_diff_min,
                "scheduled_time": scheduled_time,
                "direction": direction,
                "is_punctual": time_diff_min <= 3,
                "is_far_from_timetable": time_diff_min > 3,
            })

    print(f"Obliczono TimeDiff dla {len(results)} pomiarów")

    # Średni TimeDiff dla każdej linii
    line_avg = {}
    for row in results:
        line = row['line']
        if line not in line_avg:
            line_avg[line] = {
                'line': line,
                'total_time_diff': 0.0,
                'count': 0
            }
        line_avg[line]['total_time_diff'] += row['time_diff_minutes']
        line_avg[line]['count'] += 1

    line_summaries = []
    for line, data in line_avg.items():
        avg_time_diff = data['total_time_diff'] / data['count']
        line_summaries.append({
            'line': line,
            'dt': args.dt,
            'hour': args.hour,
            'avg_time_diff_minutes': round(avg_time_diff, 2),
            'measurement_count': data['count'],
            'total_time_diff': round(data['total_time_diff'], 2)
        })

    line_summaries.sort(key=lambda x: x['avg_time_diff_minutes'], reverse=True)

    print("\n=== ŚREDNIE ODCHYLENIE OD ROZKŁADU (TimeDiff) DLA LINII ===")
    print(f"Przeanalizowano {len(line_summaries)} linii\n")
    for i, line_data in enumerate(line_summaries, 1):
        print(f"{i}. Linia {line_data['line']}")
        print(f"   Data: {line_data['dt']}, Godzina: {line_data['hour']}")
        print(f"   Średnie odchylenie: {line_data['avg_time_diff_minutes']} min")
        print(f"   Liczba pomiarów: {line_data['measurement_count']}")
        print(f"   Suma odchyleń: {line_data['total_time_diff']} min\n")

    # Zapis do HBase
    if line_summaries:
        import happybase
        conn = happybase.Connection(host="localhost", port=8000, transport="buffered")
        table_name = "ztm_line_avg_delays"  # zostawiam nazwę, ale treść to TimeDiff
        column_families = {'cf': dict()}

        if table_name.encode() not in conn.tables():
            conn.create_table(table_name, column_families)

        table = conn.table(table_name)

        with table.batch(batch_size=1000) as b:
            for line_data in line_summaries:
                rowkey = f"{line_data['dt']}_{line_data['hour']:02d}_{line_data['line']}"
                hbase_row = {
                    b"cf:line": str(line_data['line']).encode(),
                    b"cf:dt": str(line_data['dt']).encode(),
                    b"cf:hour": str(line_data['hour']).encode(),
                    b"cf:avg_time_diff_minutes": str(line_data['avg_time_diff_minutes']).encode(),
                    b"cf:measurement_count": str(line_data['measurement_count']).encode(),
                    b"cf:total_time_diff": str(line_data['total_time_diff']).encode(),
                }
                b.put(rowkey.encode(), hbase_row)

        conn.close()
        print(f"\nZapisano {len(line_summaries)} rekordów średnich linii do HBase (ztm_line_avg_delays).")

    spark.stop()


if __name__ == "__main__":
    main()

# Komendy HBase:
# create 'ztm_line_avg_delays', 'cf'
# disable 'ztm_line_avg_delays'
# drop 'ztm_line_avg_delays'
# scan 'ztm_line_avg_delays', {LIMIT => 10}
# get 'ztm_line_avg_delays', '20240115_08_174'
#
# hbase thrift start -p 8000
# pip3 install happybase

