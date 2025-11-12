#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

INPUT="airflow/data/raw_data.txt"
OUTPUT="airflow/data/processed_data.parquet"

spark-submit \
  --conf spark.sql.shuffle.partitions=4 \
  --properties-file configs/spark-defaults.conf \
  spark_jobs/transform_job.py --input "$INPUT" --output "$OUTPUT"
