#!/bin/env bash
module load plgrid/apps/spark

PROJ_PATH=$HOME/AR/lab2/ar-pagerank
DATA_PATH=$HOME/AR/lab2/data
RESULT_PATH=$HOME/AR/lab2/results
NODE_COUNT=$(awk '{print $1}' $PBS_NODEFILE | sort | uniq | wc -l)

start-multinode-spark-cluster.sh

$SPARK_HOME/bin/spark-submit \
    --class SparkCC \
    --master spark://`hostname`:7077 \
    ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
    ${DATA_PATH}/web-BerkStan.txt $NODE_COUNT > ${RESULT_PATH}/BerkStan-${NODE_COUNT}

$SPARK_HOME/bin/spark-submit \
    --class SparkCC \
    --master spark://`hostname`:7077 \
    ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
    ${DATA_PATH}/web-Google.txt $NODE_COUNT > ${RESULT_PATH}/Google-${NODE_COUNT}

$SPARK_HOME/bin/spark-submit \
    --class SparkCC \
    --master spark://`hostname`:7077 \
    ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
    ${DATA_PATH}/web-NotreDame.txt $NODE_COUNT > ${RESULT_PATH}/NotreDame-${NODE_COUNT}

$SPARK_HOME/bin/spark-submit \
    --class SparkCC \
    --master spark://`hostname`:7077 \
    ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
    ${DATA_PATH}/web-Stanford.txt $NODE_COUNT > ${RESULT_PATH}/Stanford-${NODE_COUNT}

stop-multinode-spark-cluster.sh