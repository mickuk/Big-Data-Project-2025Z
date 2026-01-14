#!/bin/bash

# Ustawienia
HDFS_ZTM="/user/bigdata/data/csv/ztm"

echo "--- 1. Tworzenie folderu dla ZTM na HDFS ---"
hdfs dfs -mkdir -p $HDFS_ZTM

echo "--- 2. Wgrywanie pliku CSV ---"
# Łapiemy każdy plik csv zaczynający się od "ztm"
hdfs dfs -put -f ztm*.csv $HDFS_ZTM/

echo "--- 3. Tworzenie tabeli w Hive ---"
hive -f create_ztm.hql

echo "--- GOTOWE: ZTM ---"
