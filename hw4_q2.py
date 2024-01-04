'''
UserID, MovieID, Rating, Timestamp, Gender, Age, Occupation, Zip-code, Title, Genres
'''

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def map_by_key(row, key):
    dct = row.asDict()
    return ((dct['MovieID'],  dct[key]), int(dct['Rating']))


if __name__ == '__main__':
    conf = SparkConf()
    filepath = "./data/hw4.csv"
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName('HW4_Q2')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    for key in ['Gender', 'Age', 'Occupation']:
        rating_rdd = spark.read.csv(filepath, sep=',', header=True).rdd\
            .map(lambda x: map_by_key(x, key))\
            .mapValues(lambda x: (x, 1))\
            .reduceByKey(lambda x1, x2: (x1[0] + x2[0], x1[1] + x2[1]))\
            .mapValues(lambda x: x[0] / x[1])\
            .sortBy(lambda row: row[1], ascending=False)\
            .saveAsTextFile(
                os.path.join('runs', 'q2_' + key))
