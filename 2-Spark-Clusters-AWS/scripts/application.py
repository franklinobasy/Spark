#!/usr/bin/python3

#import library
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # create a spark session
    session = SparkSession \
            .builder \
            .appName("My Spark Application") \
            .getOrCreate()
    
    path = "s3://spark-bucket-0001/cities.csv"

    data = session.read.csv(path)

    data.printSchema()