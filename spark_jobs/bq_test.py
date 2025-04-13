import os
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName('testing_pyspark') \
        .getOrCreate()

    df = spark.read.options(delimiter=';', header=True).csv(os.environ['INPUT_PATH'])
    
    df.write \
        .format('bigquery') \
        .option('table', os.environ['BQ_TABLE']) \
        .mode('overwrite') \
        .save()

if __name__ == '__main__':
    main()