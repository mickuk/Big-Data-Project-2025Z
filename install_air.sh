#!/bin/bash

# Definicja ścieżki bazowej
BASE_DIR="/user/bigdata/data/parquet/open_aq"

echo "--- 1. Sprzątanie starej partycji (2026-01-05) ---"
# Usuwamy stary folder
hdfs dfs -rm -r -f $BASE_DIR/date=2026-01-05

echo "--- 2. Tworzenie nowej partycji (2026-01-13) ---"
# Tworzymy folder zgodny ze schematem partycjonowania Hive
hdfs dfs -mkdir -p $BASE_DIR/date=2026-01-13

echo "--- 3. Przenoszenie luźnego pliku do partycji ---"
# Przenosimy plik .parquet z głównego folderu do folderu z datą
# Używamy gwiazdki, żeby złapać ten długi numer w nazwie pliku
hdfs dfs -mv $BASE_DIR/air_quality_2026-01-13*.parquet $BASE_DIR/date=2026-01-13/

echo "--- 4. Aktualizacja metadanych w Hive ---"
# Uruchamiamy HQL, który naprawi tabelę (wykryje nowe foldery)
hive -f create_air_quality.hql

echo "--- GOTOWE! Sprawdzam, czy Hive widzi nową partycję: ---"
hive -e "SHOW PARTITIONS air_quality;"
hive -e "SELECT count(*) FROM air_quality;"
