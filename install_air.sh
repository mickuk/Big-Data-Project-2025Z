#!/bin/bash
echo "--- Tworzenie folderów na HDFS ---"
hdfs dfs -mkdir -p /user/bigdata/data/parquet/open_aq/date=2026-01-05

echo "--- Wgrywanie danych ---"
hdfs dfs -put *.parquet /user/bigdata/data/parquet/open_aq/date=2026-01-05/

echo "--- Tworzenie tabeli w Hive ---"
hive -f create_air_quality.hql

echo "--- GOTOWE! Sprawdź wynik poniżej: ---"
hive -e "SELECT count(*) FROM air_quality;"
