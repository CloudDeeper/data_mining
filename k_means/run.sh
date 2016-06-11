#!/bin/bash

spark-submit  --class WordCount \
              --num-executors 30 \
              target/scala-2.10/wordcount-application_2.10-1.0.jar 

rm -rf output
test -e output || mkdir output

hdfs dfs -get final/statistic_data/* output/
