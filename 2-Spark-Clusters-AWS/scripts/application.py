#!/usr/bin/python3

#import library
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # create a spark session
    session = SparkSession \
            .builder \
            .appName("My Spark Application") \
            .getOrCreate()
    
    # link to file in s3 bucket
    path = "s3://spark-bucket-0001/cities.csv"

    # Load the file from path
    data = session.read.csv(path)

    # view data schema
    data.printSchema()