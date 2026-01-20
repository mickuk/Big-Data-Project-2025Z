#!/bin/bash

# ==============================================================================
# Konfiguracja
# ==============================================================================
KAFKA_BIN="/usr/local/kafka/bin"
BOOTSTRAP_SERVER="localhost:9092"
TOPIC_NAME="api.openweather"

HDFS_DIR="/user/bigdata/data/parquet/weather_data"
HQL_SCRIPT="hive_hql/create_weather_data.hql"

# ==============================================================================
# Tworzenie topicu w Kafka
# ==============================================================================
echo "Sprawdzam czy topic Kafka istnieje: $TOPIC_NAME"

$KAFKA_BIN/kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --list | grep -w $TOPIC_NAME > /dev/null

if [ $? -ne 0 ]; then
  echo "Tworzę topic Kafka: $TOPIC_NAME"
  $KAFKA_BIN/kafka-topics.sh \
    --create \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --topic $TOPIC_NAME
else
  echo "Topic Kafka już istnieje"
fi

# ==============================================================================
# Tworzenie katalogu w HDFS
# ==============================================================================
echo "Sprawdzam katalog HDFS: $HDFS_DIR"

hdfs dfs -test -d $HDFS_DIR
if [ $? -ne 0 ]; then
  echo "Tworzę katalog w HDFS"
  hdfs dfs -mkdir -p $HDFS_DIR
else
  echo "Katalog HDFS już istnieje"
fi

# ==============================================================================
# Uruchomienie skryptu HQL
# ==============================================================================
echo "Uruchamiam skrypt Hive: $HQL_SCRIPT"

hive -f $HQL_SCRIPT

if [ $? -eq 0 ]; then
  echo "Skrypt HQL zakończony sukcesem ✓"
else
  echo "Błąd podczas wykonywania skryptu HQL ✗"
  exit 1
fi

echo "Pipeline konfiguracyjny dla danych z OpenWeather zakończony sukcesem ✓"
