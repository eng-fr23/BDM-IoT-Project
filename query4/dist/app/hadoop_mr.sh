hdfs namenode -format
"$HADOOP_HOME"/sbin/start-dfs.sh
"$HADOOP_HOME"/sbin/start-yarn.sh

hdfs dfs -put data/us_counties_covid19_daily.csv hdfs:///input
hdfs dfs -rm -r -f hdfs:///output
hadoop jar gm-bdm-query4-1.0-SNAPSHOT.jar CountyMonths hdfs:///input hdfs:///output