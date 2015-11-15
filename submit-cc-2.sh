#!/bin/env bash
module load plgrid/apps/spark

PROJ_PATH=$HOME/AR/lab2/ar-pagerank
NODE_COUNT=$(uniq $PBS_NODEFILE | wc -l | awk '{print $1}')

start-multinode-spark-cluster.sh

$SPARK_HOME/bin/spark-submit \
    --class SparkCC \
    --master spark://`hostname`:7077 \
    ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
    ${PROJ_PATH}/web-NotreDame.txt $NODE_COUNT

stop-multinode-spark-cluster.sh