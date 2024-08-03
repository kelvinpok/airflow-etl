# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType, DoubleType, LongType
from pyspark.sql import Window
from datetime import date
import os

if __name__ == '__main__':

    def Ingestion():
        # Create a SparkSession
        spark = SparkSession.builder.appName("ingestion") \
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.jars", "/spark/jars/hadoop-aws-3.3.2.jar,/spark/jars/aws-java-sdk-bundle-1.11.1026.jar") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .getOrCreate()

        yt_df = spark.read.parquet("s3a://airflow-spark-etl/Source-data/yellow_tripdata_2024-01.parquet")

        summary_data = []
        yt_cnt = yt_df.count()
        for col in yt_df.columns:
            null_pct = (yt_df.filter(yt_df[col].isNull()).count() / yt_cnt * 100) if yt_cnt > 0 else 0.0
            neg_pct = (yt_df.filter(yt_df[col] < 0).count() / yt_cnt * 100) if yt_df.schema[col].dataType in [DoubleType(), FloatType(), IntegerType(), LongType()] else 0.0
            zero_pct = (yt_df.filter(yt_df[col] == 0).count() / yt_cnt * 100) if yt_df.schema[col].dataType in [DoubleType(), FloatType(), IntegerType(), LongType()] else 0.0
            duplicate_count = yt_df.groupBy(col).count().filter(F.col('count') > 1).count()
            n_unique = yt_df.select(col).distinct().count()
            
            # Collect unique values and convert them to strings to ensure type consistency
            unique_values = [str(row[col]) for row in yt_df.select(col).distinct().limit(5).collect()]

            summary_data.append({
                'column': col,
                'data_type': str(yt_df.schema[col].dataType),
                'null_value_proportion': null_pct,
                'neg_value_proportion': neg_pct,
                '0_value_proportion': zero_pct,
                'duplicate_cnt': duplicate_count,
                'unique_value_cnt': n_unique,
                'unique_value': unique_values
            })

        summary_df = spark.createDataFrame(summary_data)
        summary_df = summary_df.select(
            'column',
            'data_type',
            F.round('null_value_proportion', 2).alias('null_value_proportion'),
            F.round('neg_value_proportion', 2).alias('neg_value_proportion'),
            F.round('0_value_proportion', 2).alias('0_value_proportion'),
            'duplicate_cnt',
            'unique_value_cnt',
            'unique_value'
        )

        summary_df.show()

        yt_df.coalesce(1).write.mode("overwrite").parquet('s3a://airflow-spark-etl/Raw-zone/yellow-taxi/202401/')

    Ingestion()
    os.system('kill %d' % os.getpid())