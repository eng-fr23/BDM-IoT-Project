"""
For each user location, find the day that had the most tweets about covid
"""

from pyspark import SparkConf, SparkContext, sql
import datetime
import pymongo
import json

# Indexes in csv

location = 1
date = 8

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
    only_date = fmt_date.split(" ")[0]
    return only_date.split("-")[2]


def main():
    conf = (SparkConf()
            .setMaster("local")
            .setAppName("MostTweets")
            )

    sc = SparkContext(conf=conf)
    sql_ctx = sql.SQLContext(sc)

    csv = sql_ctx.read.csv(app_conf["targetFile"], header=True).rdd.map(tuple)\
        .filter(lambda line_elems:  validate_date(line_elems[date]))\
        .map(lambda line_elems: (line_elems[location], day_from_date(line_elems[date])))\
        .take(50)

    print csv


if __name__ == "__main__":
    main()

