import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

def main(input_path: str, output_path: str):
    spark = SparkSession.builder.appName("ETL Spark SQL").getOrCreate()

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    df_clean = df.filter(col("revenue") > 0)
    result = df_clean.groupBy("region").agg(avg("revenue").alias("avg_revenue"))

    result.coalesce(1).write.mode("overwrite").parquet(output_path)
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    main(args.input, args.output)
