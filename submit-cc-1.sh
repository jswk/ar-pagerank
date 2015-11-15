#!/bin/env bash
module load plgrid/apps/spark

PROJ_PATH=$HOME/AR/lab2/ar-pagerank
DATA_PATH=$HOME/AR/lab2/data

$SPARK_HOME/bin/spark-submit \
    --class SparkCC \
    --master local[*] \
    ${PROJ_PATH}/target/scala-2.10/sparkpagerank_2.10-1.0.jar \
    ${DATA_PATH}/web-NotreDame.txt 1

