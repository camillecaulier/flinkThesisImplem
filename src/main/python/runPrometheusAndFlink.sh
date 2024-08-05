#!/bin/bash

source_parallelism=$1
main_parallelism=$2
aggregator_parallelism=$3
dataFileDirectory=$4
javaSource=$5

python fakePrometheus.py > throughput.dat &
PROMETHEUS_PID=$!
./flink-1.18.1/bin/flink run flinkImplemProject.jar $source_parallelism $main_parallelism $aggregator_parallelism $dataFileDirectory $javaSource | tee output.dat
sleep 30

kill $PROMETHEUS_PID
echo "Prometheus and Flink have been stopped"