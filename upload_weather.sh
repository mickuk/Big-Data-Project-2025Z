#!/bin/bash

# Ustawienia
HDFS_WEATHER="/user/bigdata/data/parquet/weather_data"
PARTITION_DATE="2026-01-13"

echo "--- 1. Tworzenie tabeli weather_data (z Twojego pliku HQL) ---"
hive -f create_weather_data.hql

echo "--- 2. Tworzenie folderu partycji na HDFS ---"
# Tworzymy folder: .../weather_data/date=2026-01-13
hdfs dfs -mkdir -p $HDFS_WEATHER/date=$PARTITION_DATE

echo "--- 3. Wgrywanie pliku Parquet ---"
# Używam gwiazdki (*) - zadziała dla weather_warsaw.parquet, weather_data.parquet itp.
hdfs dfs -put -f weather*.parquet $HDFS_WEATHER/date=$PARTITION_DATE/

echo "--- 4. Aktualizacja Hive (Wykrycie nowej partycji) ---"
hive -e "MSCK REPAIR TABLE weather_data;"

echo "--- GOTOWE: Pogoda ---"
