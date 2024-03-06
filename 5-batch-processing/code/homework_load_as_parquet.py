#!/usr/bin/env python
# coding: utf-8


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
import pandas as pd

# download & unzip file
!wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-10.csv.gz


!gzip -d fhv_tripdata_2019-10.csv.gz

# Create Spark Dataframe
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


df_pd = pd.read_csv('fhv_tripdata_2019-10.csv')

schema = types.StructType([
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropOff_datetime', types.TimestampType(), True), 
    types.StructField('PUlocationID', types.IntegerType(), True), 
    types.StructField('DOlocationID', types.IntegerType(), True), 
    types.StructField('SR_Flag', types.StringType(), True), 
    types.StructField('Affiliated_base_number', types.StringType(), True)
])


df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhv_tripdata_2019-10.csv')

# Trim unneccessary spaces
df = df \
    .withColumn('dispatching_base_num', F.trim('dispatching_base_num')) \
    .withColumn('Affiliated_base_number', F.trim('Affiliated_base_number'))

# Rename colums
df = df \
    .withColumnRenamed('dropOff_datetime', 'dropoff_datetime') \
    .withColumnRenamed('Affiliated_base_number', 'affiliated_base_number')

# Replace #N/A by NULL
df = df.replace("#N/A", None)


# Save parquet file
df \
    .repartition(6) \
    .write.parquet('/home/huely/data/pq/fhv_homework', mode='overwrite')





