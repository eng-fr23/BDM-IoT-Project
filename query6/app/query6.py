"""
- Dataset: https://www.kaggle.com/sudalairajkumar/covid19-in-usa?select=us_states_covid19_daily.csv
- Query: For each state, find the month that had the highest increase in hospitalized patients
"""
from pyspark.streaming import StreamingContext
from pyspark import SparkContext
import pymongo
import json

with open("conf.json", "r") as conf_file:
    conf = json.loads(conf_file.read())

appName = conf["appName"]
parallelismDegree = "local[%d]" % conf["parallelismDegree"]
batchInterval = conf["batchInterval"]


def send_rdd(rdd_partition):
    db_conf_str = "mongodb://%s:%d" % (conf["dbAddr"], conf["dbPort"])
    collection = pymongo.MongoClient(db_conf_str)[conf["dbName"]][conf["dbCollection"]]
    for elem in rdd_partition:
        collection.insert_one({"location": elem[0], "day": elem[1]})


def main():
    ctx = SparkContext(parallelismDegree, appName)
    sctx = StreamingContext(ctx, batchInterval)

    data = sctx.socketTextStream(hostname="localhost", port=conf["recv-port"])
    

if __name__ == "__main__":
    main()
