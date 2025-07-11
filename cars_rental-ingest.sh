#!/bin/bash

wget -O ~/examen-final/CarRentalData.csv https://data-engineer-edvai-public.s3.amazonaws.com/CarRentalData.csv
wget -O ~/examen-final/georef-united-states-of-america.csv" https://data-engineer-edvai-public.s3.amazonaws.com/georef-united-states-of-america-state.csv

/home/hadoop/hadoop/bin/hdfs dfs -mkdir /ingest/automobiles
/home/hadoop/hadoop/bin/hdfs dfs -put -f ~/examen-final/CarRentalData.csv /ingest/automobiles
/home/hadoop/hadoop/bin/hdfs dfs -put -f ~/examen-final/georef-united-states-of-america.csv /ingest/automobiles

rm ~/examen-final/*.csv
