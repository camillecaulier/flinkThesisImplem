#!/bin/bash

main_parallelism=$1
dataFileDirectory=$2
javaSource=$3

python fakePrometheusBusynessPerVertex.py > throughput.dat &
PROMETHEUS_PID=$!
./flink-1.18.1/bin/flink run flinkImplemProject.jar $main_parallelism $dataFileDirectory $javaSource | tee output.dat

kill $PROMETHEUS_PID
echo "Prometheus and Flink have been stopped"