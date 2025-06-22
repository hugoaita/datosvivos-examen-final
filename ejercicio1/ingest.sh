#!/bin/bash
wget -O 2021-informe-ministerio.csv \
        https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv 
wget -O 202206 202206-informe-ministerio.csv \
        https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv
wget -O aeropuertos_detalle.csv \
        https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv

/home/hadoop/hadoop/bin/hdfs dfs -mkdir  /ingest/vuelos
/home/hadoop/hadoop/bin/hdfs dfs -put -f 2021-informe-ministerio.csv /ingest/vuelos/2021-informe-ministerio.csv
/home/hadoop/hadoop/bin/hdfs dfs -put -f 202206-informe-ministerio.csv /ingest/vuelos/202206-informe-ministerio.csv
/home/hadoop/hadoop/bin/hdfs dfs -put -f aeropuertos_detalle.csv /ingest/vuelos/aeropuertos_detalle.csv

