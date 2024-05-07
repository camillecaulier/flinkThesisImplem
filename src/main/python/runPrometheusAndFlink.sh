#!/bin/bash

main_parallelism=$1
dataFileDirectory=$2

python fakePrometheus.py > throughput.dat &
PROMETHEUS_PID=$!
./flink-1.18.1/bin/flink run flinkImplemProject.jar $main_parallelism $dataFileDirectory | tee output.dat

kill $PROMETHEUS_PID
echo "Prometheus and Flink have been stopped"