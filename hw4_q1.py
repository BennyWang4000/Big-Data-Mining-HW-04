from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


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
    # UserID::MovieID::Rating::Timestamp

    rating_rdd = spark.read.csv(filepath, sep=',', header=True)\
        .rdd.flatMap(lambda x: ratings_fun(x))\
        .reduceByKey(lambda x1, x2: x1+x2)\
            .sortBy(lambda x: x[1], ascending=False)
    print(rating_rdd.collect())
