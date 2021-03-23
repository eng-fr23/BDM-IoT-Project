"""
- Dataset: https://www.kaggle.com/andradaolteanu/covid-19-sentiment-analysis-social-networks/data?select=covid19_tweets.csv
- Query: For each user location, find the day that had the most tweets about covid
"""

from operator import add
from pyspark import sql
import datetime
import pymongo
import json


# Csv indexes
location = 1
date = 8

# Init configurations
with open("conf.json", "r") as app_conf_file:
    app_conf = json.loads(app_conf_file.read())


def validate_date(fmt_date):
    """
    Checks that the date is in the specified format.
    :param fmt_date: date that must be in the yyyy-mm-dd hh:mm:ss format
    :return: bool
    """
    try:
        datetime.datetime.strptime(fmt_date, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return False
    return True


def day_from_date(fmt_date):
    """
    Extracts the day from the passed date
    :param fmt_date: the previously valitaded string representation of a date
    :return: a string representing a day in the yyyy-mm-dd format
    """
    return fmt_date.split(" ")[0]


def write_to_mongo(rdd_partition):
    """
    Writes every row instance of an RDD partition onto a mongodb collection.
    :param rdd_partition: the partition to be written
    :return: None
    """
    db_conf_str = "mongodb://%s:%d" % (app_conf["dbAddr"], app_conf["dbPort"])
    collection = pymongo.MongoClient(db_conf_str)[app_conf["dbName"]][app_conf["dbCollection"]]
    for elem in rdd_partition:
        collection.insert_one({"location": elem[0], "day": elem[1]})


def main():
    # SparkSession is used instead of SparkContext because it provides native csv
    # parsing capabilities, which are needed because the csv used in this app uses
    # commas in the fields, escaping them with double quotes. SparkSession is used
    # to work with DataFrames, but it encapsulates a SparkContext and its rdd
    # manipulation capabilities are recovered through the rdd parameter.
    sql_ses = sql.SparkSession\
        .builder\
        .appName("MostTweetsByLocation")\
        .master("local")\
        .getOrCreate()

    # Loading a csv with the header=True argument makes it possible wto use the first row as the schema.
    # The dataframe is then transformed into an RDD to recover the normal SparkContext capabilities,
    # through which some transformations are made upon the dataset, namely the map/reduceByKey used
    # to count number of tweets, and the max computation. The data is then saved into a mongodb collection.
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
        .foreachPartition(write_to_mongo)


if __name__ == "__main__":
    main()

