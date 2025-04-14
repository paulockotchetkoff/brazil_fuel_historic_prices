import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True)
    parser.add_argument('--bq_table', required=True)
    parser.add_argument('--temp_bucket', required=True)
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName('testing-pyspark') \
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
        .getOrCreate()

    df = spark.read \
        .options(
            delimiter=';',
            header=True,
            inferSchema=True,
            encoding='UTF-8',
            dateFormat='dd/MM/yyyy'
        ) \
        .csv(args.input_path)
    print(f'Total records: {df.count()}')


    df.write.format('bigquery') \
        .option('table', args.bq_table) \
        .option('temporaryGcsBucket', args.temp_bucket) \
        .mode('overwrite') \
        .save()

if __name__ == '__main__':
    main()

# .option('writeDisposition', 'WRITE_TRUNCATE') \
# .option('partitionField', 'date') \
# .option('clusteredFields', 'state,fuel_type') \