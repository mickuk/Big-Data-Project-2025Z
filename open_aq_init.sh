#!/bin/bash

# ==============================================================================
# Skrypt inicjalizujący środowisko Big Data (Kafka, HDFS, Hive) dla danych open_aq
# Projekt: Platforma Big Data dla miejskich danych środowiskowych i transportowych
# ==============================================================================

# 1. Instrukcja wstępna
echo "======================================================================"
echo "UWAGA: Upewnij się, że w bieżącym katalogu znajdują się pliki danych:"
echo " - air_quality_2026-01-13_*.parquet"
echo " - weather_*.parquet"
echo " - ztm*.csv"
echo "oraz pliki HQL: create_air_quality.hql, create_weather_data.hql, create_ztm.hql"
echo "======================================================================"
echo "Czekam 3 sekundy..."
sleep 3

# ------------------------------------------------------------------------------
# SEKCJA 1: KONFIGURACJA KAFKA (Twój zakres: open_aq_topic)
# ------------------------------------------------------------------------------
echo ">>> [1/4] Konfiguracja Apache Kafka..."

# Ustawienia zmiennych dla Kafki (dostosuj ścieżki jeśli inne środowisko)
ZOOKEEPER_HOST="localhost:2181"

# Tworzenie tematu dla OpenAQ (Twoje zadanie)
# Zgodnie z PDF: acks=0, format JSON (ale tutaj tworzymy tylko topic)
kafka-topics.sh --create --zookeeper $ZOOKEEPER_HOST --replication-factor 1 --partitions 1 --topic open_aq_topic --if-not-exists
echo "Utworzono/Sprawdzono topic: open_aq_topic"

# ------------------------------------------------------------------------------
# SEKCJA 2: OPEN AQ
# ------------------------------------------------------------------------------
echo ">>> [2/4] Wgrywanie danych OPEN AQ i tworzenie tabeli Hive..."

# Ustawienia ścieżek
HDFS_OPENAQ_BASE="/user/bigdata/data/parquet/open_aq"
PARTITION_DATE="2026-01-13"
HDFS_OPENAQ_PARTITION="$HDFS_OPENAQ_BASE/date=$PARTITION_DATE"

# A. Sprzątanie (na wypadek ponownego uruchomienia)
echo "   -> Sprzątanie starej partycji ($PARTITION_DATE)..."
hdfs dfs -rm -r -f $HDFS_OPENAQ_PARTITION

# B. Tworzenie katalogu partycji
# Tworzymy folder 'date=2026-01-13' co pozwoli Hive automatycznie rozpoznać partycję
echo "   -> Tworzenie struktury katalogów na HDFS..."
hdfs dfs -mkdir -p $HDFS_OPENAQ_PARTITION

# C. Wgrywanie pliku
echo "   -> Wgrywanie pliku Parquet OpenAQ..."
# Używamy -put (kopiowanie z lokalnego systemu plików na HDFS)
hdfs dfs -put -f air_quality_*.parquet $HDFS_OPENAQ_PARTITION/

# D. Hive - Tworzenie tabeli i naprawa partycji
echo "   -> Tworzenie tabeli 'air_quality' w Hive..."
hive -f create_air_quality.hql

echo "   -> Aktualizacja metadanych (MSCK REPAIR)..."
hive -e "MSCK REPAIR TABLE air_quality;"

# E. Weryfikacja
echo "   -> Weryfikacja OpenAQ:"
hive -e "SELECT count(*) FROM air_quality;"

# ------------------------------------------------------------------------------
# SEKCJA 3: WEATHER (Integracja skryptu upload_weather.sh)
# ------------------------------------------------------------------------------
echo ">>> [3/4] Wgrywanie danych WEATHER..."

HDFS_WEATHER="/user/bigdata/data/parquet/weather_data"
WEATHER_PARTITION_DATE="2026-01-13"

# Tworzenie tabeli (jeśli nie istnieje)
hive -f create_weather_data.hql

# Tworzenie katalogu partycji
hdfs dfs -mkdir -p $HDFS_WEATHER/dt=$WEATHER_PARTITION_DATE

# Wgrywanie danych
echo "   -> Wgrywanie plików Weather..."
hdfs dfs -put -f weather*.parquet $HDFS_WEATHER/dt=$WEATHER_PARTITION_DATE/

# Naprawa partycji
hive -e "MSCK REPAIR TABLE weather_data;"

# ------------------------------------------------------------------------------
# SEKCJA 4: ZTM (Integracja skryptu upload_ztm.sh)
# ------------------------------------------------------------------------------
echo ">>> [4/4] Wgrywanie danych ZTM..."

HDFS_ZTM="/user/bigdata/data/csv/ztm"

# Tworzenie katalogu
hdfs dfs -mkdir -p $HDFS_ZTM

# Wgrywanie danych CSV
echo "   -> Wgrywanie plików ZTM CSV..."
hdfs dfs -put -f ztm*.csv $HDFS_ZTM/

# Tworzenie tabeli
hive -f create_ztm.hql

echo "======================================================================"
echo ">>> PROCES ZAKOŃCZONY POMYŚLNIE <<<"
echo "Możesz sprawdzić poprawność danych w Hive wpisując: hive"
echo "======================================================================"
