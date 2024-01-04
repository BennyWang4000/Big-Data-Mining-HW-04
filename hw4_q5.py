# %%
'''
UserID, MovieID, Rating, Timestamp, Gender, Age, Occupation, Zip-code, Title, Genres
'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import math
import argparse
import os


def map_by_user(row, rating_dct: Dict[str, int]):
    dct = row.asDict()
    return (dct['MovieID'], ([int(dct['Rating'])], [int(rating_dct.get(dct['UserID'], 0))]))


def filter_by_movie(row, movie):
    dct = row.asDict()
    return dct['MovieID'] == movie


def map_by_movie(row):
    dct = row.asDict()
    return (dct['UserID'], int(dct['Rating']))


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
        'spark://0.0.0.0:8080').setAppName('HW4_Q5')
    sc = SparkContext(conf=conf)
    spark = SparkSession(sc)

    parser = argparse.ArgumentParser()
    parser.add_argument('--movie', default='1193', type=str)
    args = parser.parse_args()

    movie_user = spark.read.csv(filepath, sep=',', header=True).rdd\
        .filter(lambda row: filter_by_movie(row, args.movie))\
        .map(lambda row: map_by_movie(row))\
        .collect()

    rating_rdd = spark.read.csv(filepath, sep=',', header=True).rdd\
        .map(lambda row: map_by_user(row, dict(movie_user)))\
        .reduceByKey(lambda movie1, movie2: (movie1[0] + movie2[0], movie1[1] + movie2[1]))\
        .mapValues(lambda movie: cos_sim(movie[0], movie[1]))\
        .sortBy(lambda row: row[1], ascending=False)\
        .saveAsTextFile(
            os.path.join('runs', 'q5'))


# %%
