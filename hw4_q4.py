# %%
'''
UserID, MovieID, Rating, Timestamp, Gender, Age, Occupation, Zip-code, Title, Genres
'''

import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import math
import argparse


def map_by_movie(row, rating_dct: Dict[str, int]):
    dct = row.asDict()
    return (dct['UserID'], ([int(dct['Rating'])], [int(rating_dct.get(dct['MovieID'], 0))]))


def filter_by_user(row, user):
    dct = row.asDict()
    return dct['UserID'] == user


def map_by_user(row):
    dct = row.asDict()
    return (dct['MovieID'], int(dct['Rating']))


def cos_sim(v1, v2):
    sumxx, sumxy, sumyy = 0, 0, 0
    for i in range(len(v1)):
        x = v1[i]
        y = v2[i]
        sumxx += x*x
        sumyy += y*y
        sumxy += x*y
    return 0 if math.sqrt(sumxx*sumyy) == 0 else sumxy/math.sqrt(sumxx*sumyy)


# %%
if __name__ == '__main__':
    conf = SparkConf()
    filepath = "./data/hw4.csv"
    conf.setMaster(
        'spark://0.0.0.0:8080').setAppName('HW4_Q4')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    parser = argparse.ArgumentParser()
    parser.add_argument('--user', default='1', type=str)
    args = parser.parse_args()

    user_movie = spark.read.csv(filepath, sep=',', header=True).rdd\
        .filter(lambda row: filter_by_user(row, args.user))\
        .map(lambda row: map_by_user(row))\
        .collect()

    rating_rdd = spark.read.csv(filepath, sep=',', header=True).rdd\
        .map(lambda row: map_by_movie(row, dict(user_movie)))\
        .reduceByKey(lambda user1, user2: (user1[0] + user2[0], user1[1] + user2[1]))\
        .mapValues(lambda user: cos_sim(user[0], user[1]))\
        .sortBy(lambda row: row[1], ascending=False)\
        .saveAsTextFile(
            os.path.join('runs', 'q4'))


# %%
