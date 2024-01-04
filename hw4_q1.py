import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def map_by_movie(row):
    dct = row.asDict()
    return (dct['MovieID'], int(dct['Rating']))


def ratings_fun(row):
    lst = []
    for word in row.asDict()['MovieID'].split(' '):
        lst.append(((word, row.asDict()['Title'], row.asDict()['Rating']), 1))
    return lst


if __name__ == '__main__':
    conf = SparkConf()
    filepath = "./data/hw4.csv"
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName('HW4_Q1')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    rating_rdd = spark.read.csv(filepath, sep=',', header=True).rdd\
        .map(lambda row: map_by_movie(row))\
        .mapValues(lambda row: (row, 1))\
        .reduceByKey(lambda row1, row2: (row1[0] + row2[0], row1[1] + row2[1]))\
        .mapValues(lambda row: row[0] / row[1])\
        .sortBy(lambda row: row[1], ascending=False)\
        .saveAsTextFile(
            os.path.join('runs', 'q1'))
