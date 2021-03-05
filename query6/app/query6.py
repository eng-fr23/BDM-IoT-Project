"""
- Dataset: https://www.kaggle.com/sudalairajkumar/covid19-in-usa?select=us_states_covid19_daily.csv
- Query: For each state, find the month that had the highest increase in hospitalized patients
"""
from pyspark.streaming import StreamingContext
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


def month_from_date(fmt_date):
    # date fmt is yyyymmdd
    return fmt_date[4:6]


def send_rdd(rdd_partition):
    db_conf_str = "mongodb://%s:%d" % (conf["dbAddr"], conf["dbPort"])
    collection = pymongo.MongoClient(db_conf_str)[conf["dbName"]][conf["dbCollection"]]
    for elem in rdd_partition:
        collection.insert_one({"location": elem[0], "day": elem[1]})


def validate_int(n):
    try:
        int(n)
    except ValueError:
        return False
    return True


def main():
    ctx = SparkContext(parallelismDegree, appName)
    sctx = StreamingContext(ctx, batchInterval)

    data = sctx.socketTextStream(hostname="localhost", port=conf["recv-port"])\
        .map(lambda line: line.split(","))\
        .map(lambda splitted: ((splitted[state], month_from_date(splitted[date])), splitted[hospitalized_cum]))\
        .filter(lambda line_tuple: validate_int(line_tuple[1]))\
        .map(lambda line_tuple: (line_tuple[0], int(line_tuple[1])))\
        .reduceByKey(operator.add)

    # foreachrdd

    data.pprint()
    sctx.start()
    sctx.awaitTermination()


if __name__ == "__main__":
    main()
