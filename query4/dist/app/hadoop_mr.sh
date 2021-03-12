#!/usr/bin/env bash
# hadoop_mr.sh - setup script for the hadoop environment
#
# This script initializes the environment set up through docker or locally by
# formatting the hdfs namenode and running the hadoop dfs/yarn init scripts.
# After that, the dataset taken in examination is uploaded onto the distributed
# file system and the map reduce app is executed, writing its results onto the
# output directory on the dfs.

hdfs namenode -format
"$HADOOP_HOME"/sbin/start-dfs.sh
"$HADOOP_HOME"/sbin/start-yarn.sh

hdfs dfs -put data/us_counties_covid19_daily.csv hdfs:///input
hdfs dfs -rm -r -f hdfs:///output
hadoop jar gm-bdm-query4-1.0-SNAPSHOT.jar CountyMonths hdfs:///input hdfs:///output