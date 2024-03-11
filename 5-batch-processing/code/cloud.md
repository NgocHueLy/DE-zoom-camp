

# for submitting dataproc spark job through CLI - terminal

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    gs://de-zoomcamp-nytaxi-w5/code/10_spark_sql_local_cluster_parser-multiple_clusters.py \
    -- \
        --input_green=gs://de-zoomcamp-nytaxi-w5/pq/green/2020/*/ \
        --input_yellow=gs://de-zoomcamp-nytaxi-w5/pq/yellow/2020/*/ \
        --output=gs://de-zoomcamp-nytaxi-w5/report-2020/

# Dataproc - write spark job result to Bigquery
https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark


ADD THIS TO THE PYTHON SCRIPT 
```
-- Save the data to BigQuery
word_count.write.format('bigquery') \
  .option('table', 'wordcount_dataset.wordcount_output') \
  .save()
```


dtc-de-course-412502.ny_taxi.reports-2020

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    gs://de-zoomcamp-nytaxi-w5/code/10_spark_sql_parser_bigquery.py \
    -- \
        --input_green=gs://de-zoomcamp-nytaxi-w5/pq/green/2020/*/ \
        --input_yellow=gs://de-zoomcamp-nytaxi-w5/pq/yellow/2020/*/ \
        --output=dtc-de-course-412502.ny_taxi.report_2020