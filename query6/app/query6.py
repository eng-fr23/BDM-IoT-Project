"""
- Dataset: https://www.kaggle.com/sudalairajkumar/covid19-in-usa?select=us_states_covid19_daily.csv
- Query: For each state, find the month that had the highest increase in hospitalized patients
"""
from pyspark.sql.functions import when, lag, isnull
from pyspark.streaming import StreamingContext
from pyspark.sql import Window, SparkSession
from pyspark import SparkContext
import operator
import pymongo
import json

# Indexes in csv
date = 0
state = 1
hospitalized_cum = 9

with open("conf.json", "r") as conf_file:
    conf = json.loads(conf_file.read())

appName = conf["appName"]
parallelismDegree = "local[%d]" % conf["parallelismDegree"]
batchInterval = conf["batchInterval"]


def get_sparksession_instance(spark_conf):
    # from the official spark examples repo
    if "sparkSessionSingletonInstance" not in globals():
        globals()["sparkSessionSingletonInstance"] = SparkSession\
            .builder\
            .config(conf=spark_conf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def month_from_date(fmt_date):
    # date fmt is yyyymmdd
    return fmt_date[4:6]


def send_rdd(rdd_partition):
    db_conf_str = "mongodb://%s:%d" % (conf["dbAddr"], conf["dbPort"])
    collection = pymongo.MongoClient(db_conf_str)[conf["dbName"]][conf["dbCollection"]]
    for elem in rdd_partition:
        collection.insert_one({"state": elem[0], "monthly_increase": elem[1]})


def validate_int(n):
    try:
        int(n)
    except ValueError:
        return False
    return True


def extract_diff(time, rdd):
    """
    Transform to dataframe to compute monthly increase
    """
    try:
        if rdd.isEmpty():
            return

        # use a window to force an order by state and month so to have each monthly
        # data close to the next
        spark = get_sparksession_instance(rdd.context.getConf())
        df = spark.createDataFrame(rdd, ["state", "month", "hospitalized"])
        win_conf = Window.partitionBy().orderBy("state", "month")

        # add two columns, the first by lagging one row foreach entry so to catch
        # the hospitalized count previous month, the second one just computes the difference
        # (aka the increase, possibly negative) considering the null corner case too
        df = df.withColumn("prev_hospitalized", lag(df.hospitalized).over(win_conf))
        df = df.withColumn("increment", when(
                isnull(df.hospitalized - df.prev_hospitalized), 0)
                .otherwise(df.hospitalized - df.prev_hospitalized)
            )

        # back to an rdd to compute the max among the increases by state, then save it onto
        # a mongodb database instance
        df.rdd\
            .map(lambda line: (line[0], line[4]))\
            .reduceByKey(max)\
            .foreachPartition(send_rdd)
    finally:
        pass


def main():
    ctx = SparkContext(parallelismDegree, appName)
    sctx = StreamingContext(ctx, batchInterval)

    # Recv data, calculate total hospitalized by month
    # The following acquires streamed data from the text socket, interprets it as csv
    # and computes the total number of cases by month, to then pass the info to the extract_diff function
    sctx.socketTextStream(hostname=conf["recvAddr"], port=conf["recvPort"]) \
        .map(lambda line: line.split(",")) \
        .map(lambda splitted: ((splitted[state], month_from_date(splitted[date])), splitted[hospitalized_cum])) \
        .filter(lambda line_tuple: validate_int(line_tuple[1])) \
        .map(lambda line_tuple: (line_tuple[0], int(line_tuple[1]))) \
        .reduceByKey(operator.add) \
        .map(lambda line_tuple: (line_tuple[0][0], line_tuple[0][1], int(line_tuple[1])))\
        .foreachRDD(extract_diff)

    sctx.start()
    sctx.awaitTermination()


if __name__ == "__main__":
    main()
