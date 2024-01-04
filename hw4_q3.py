'''
UserID, MovieID, Rating, Timestamp, Gender, Age, Occupation, Zip-code, Title, Genres
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def map_by_user(row):
    dct = row.asDict()
    return (['UserID'], int(dct['Rating']))


def map_by_genre(row):
    dct = row.asDict()
    res = []
    for genre in dct['Genres'].split('|'):
        res.append(((dct['UserID'], genre), int(dct['Rating'])))
    return res


if __name__ == '__main__':
    conf = SparkConf()
    filepath = "./data/hw4.csv"
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName('HW4_Q3')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    # ---------------------------------------------------------------------------- #
    #                                     user                                     #
    # ---------------------------------------------------------------------------- #
    rating_rdd = spark.read.csv(filepath, sep=',', header=True).rdd\
        .map(lambda x: map_by_user(x))\
        .mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda x1, x2: (x1[0] + x2[0], x1[1] + x2[1]))\
        .mapValues(lambda x: x[0] / x[1])

    # ---------------------------------------------------------------------------- #
    #                                     genre                                    #
    # ---------------------------------------------------------------------------- #
    rating_rdd = spark.read.csv(filepath, sep=',', header=True).rdd\
        .flatMap(lambda x: map_by_genre(x))\
        .mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda x1, x2: (x1[0] + x2[0], x1[1] + x2[1]))\
        .mapValues(lambda x: x[0] / x[1])

    print(rating_rdd.collect())
