import argparse 
import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', required=True)
    parser.add_argument('--bq_table', required=True)
    parser.add_argument('--temp_bucket', required=True)
    args = parser.parse_args()

    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    logger.info('Initializing Spark session')
    spark = SparkSession.builder \
        .appName('testing-pyspark') \
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0') \
        .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
        .getOrCreate()
    
    logger.info(f'Spark version: {spark.version}')

    logger.info(f'Attempting to read: {args.input_path}')
    # df = spark.read \
    #     .options(
    #         delimiter=';',
    #         header=True,
    #         inferSchema=True,
    #         encoding='UTF-8',
    #         dateFormat='dd/MM/yyyy'
    #     ) \
    #     .csv(args.input_path)
    df = spark.read.option('header', 'true').option('sep', ';').csv(args.input_path)
    
    logger.info('CSV read successful')

    logger.info(f'Writing to BigQuery table: {args.bq_table}')
    df.write.format('bigquery') \
        .option('table', args.bq_table) \
        .option('temporaryGcsBucket', args.temp_bucket) \
        .mode('overwrite') \
        .save()
    
    logger.info('Write to BigQuery completed successfully')

if __name__ == '__main__':
    main()

# .option('writeDisposition', 'WRITE_TRUNCATE') \
# .option('partitionField', 'date') \
# .option('clusteredFields', 'state,fuel_type') \