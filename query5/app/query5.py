"""
- Dataset: https://www.kaggle.com/andradaolteanu/covid-19-sentiment-analysis-social-networks/data?select=covid19_tweets.csv
- Query: For each user location, find the day that had the most tweets about covid
"""

from operator import add
from pyspark import sql
import datetime
import pymongo
import json


# Indexes in csv
location = 1
date = 8

# Init configurations
with open("conf.json", "r") as app_conf_file:
    app_conf = json.loads(app_conf_file.read())


def validate_date(fmt_date):
    try:
        datetime.datetime.strptime(fmt_date, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    return True


def day_from_date(fmt_date):
    # date fmt is yyyy-mm-dd hh:mm:ss
    # split(" ")[0] -> yyyy-mm-dd
    return fmt_date.split(" ")[0]


def send_rdd(rdd_partition):
    db_conf_str = "mongodb://%s:%d" % (app_conf["dbAddr"], app_conf["dbPort"])
    collection = pymongo.MongoClient(db_conf_str)[app_conf["dbName"]][app_conf["dbCollection"]]
    for elem in rdd_partition:
        collection.insert_one({"location": elem[0], "day": elem[1]})


def main():
    sql_ses = sql.SparkSession\
        .builder\
        .appName("MostTweetsByLocation")\
        .master("local")\
        .getOrCreate()

    sql_ses\
        .read\
        .csv(app_conf["targetFile"], header=True)\
        .rdd\
        .map(tuple)\
        .filter(lambda line_elems: validate_date(line_elems[date]))\
        .map(lambda line_elems: ((line_elems[location], day_from_date(line_elems[date])), 1))\
        .reduceByKey(add)\
        .map(lambda tuple_elems: (tuple_elems[0][0], (tuple_elems[0][1], tuple_elems[1])))\
        .reduceByKey(lambda x, y: x if x[1] > y[1] else y)\
        .map(lambda elems: (elems[0], elems[1][0]))\
        .foreachPartition(send_rdd)


if __name__ == "__main__":
    main()

