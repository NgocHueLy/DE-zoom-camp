#!/usr/bin/env python
# coding: utf-8

import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder \
    .master("spark://de-zoomcamp.us-west1-b.c.dtc-de-course-412502.internal:7077") \
    .appName('test') \
    .getOrCreate()

# Combine both datasets

# create dataset for green and yellow taxi
df_green = spark.read.parquet('/home/huely/data/pq/green/*/*')

df_yellow = spark.read.parquet('/home/huely/data/pq/yellow/*/*')

# rename columns for append
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')

# find common columns 

common_columns = ['VendorID',
 'pickup_datetime',
 'dropoff_datetime',
 'store_and_fwd_flag',
 'RatecodeID',
 'PULocationID',
 'DOLocationID',
 'passenger_count',
 'trip_distance',
 'fare_amount',
 'extra',
 'mta_tax',
 'tip_amount',
 'tolls_amount',
 'improvement_surcharge',
 'total_amount',
 'payment_type',
 'congestion_surcharge']


# add service_type col to indicate taxi service & union both datasets



df_green_select = df_green \
    .select(common_columns) \
    .withColumn('service_type', F.lit('green'))


df_yellow_select = df_yellow \
    .select(common_columns) \
    .withColumn('service_type', F.lit('yellow'))

df_trips_data = df_green_select.unionAll(df_yellow_select)


# Query with SQL

df_trips_data.createOrReplaceTempView('trips_data')

df_result = spark.sql("""
SELECT
    -- Reveneue grouping 
    PULocationID AS revenue_zone,
    date_trunc("month", pickup_datetime) AS revenue_month, 

    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,

    -- Additional calculations
    AVG(pASsenger_count) AS avg_monthly_pASsenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance

FROM trips_data
GROUP BY 1,2,3

""")


# write result to parquet file:
df_result.write.parquet('/home/huely/data/report/revenue/', mode='overwrite')
