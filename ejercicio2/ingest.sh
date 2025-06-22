#!/bin/bash

wget -O /home/hadoop/examen-final/ejercicio2/CarRentalData.csv https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv
wget -O "/home/hadoop/examen-final/ejercicio2/georef-united-states-of-america.csv" https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv

/home/hadoop/hadoop/bin/hdfs dfs -mkdir /ingest/automobiles
/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/examen-final/ejercicio2/CarRentalData.csv /ingest/automobiles
/home/hadoop/hadoop/bin/hdfs dfs -put -f /home/hadoop/examen-final/ejercicio2/georef-united-states-of-america.csv /ingest/automobiles
