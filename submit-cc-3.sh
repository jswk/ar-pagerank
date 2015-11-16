#!/bin/env bash
module load plgrid/apps/spark

PROJ_PATH=$HOME/AR/lab2/ar-pagerank
DATA_PATH=$HOME/AR/lab2/data
RESULT_PATH=$HOME/AR/lab2/results3

start-multinode-spark-cluster.sh

for NODES in {2..24..2}; do
    ${SPARK_HOME}/bin/spark-submit \
        --class SparkCC \
        --master spark://`hostname`:7077 \
        ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
        ${DATA_PATH}/web-NotreDame.txt ${NODES} > ${RESULT_PATH}/NotreDame-${NODES}
done

stop-multinode-spark-cluster.sh