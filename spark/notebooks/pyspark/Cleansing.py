# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType, DoubleType, LongType
from pyspark.sql import Window
from datetime import date
import os

if __name__ == '__main__':

    def _to_string(df, cols:list):
        for _col in cols:
            df = df.withColumn(_col, F.col(_col).cast(StringType()))
        return df

    def _to_double(df, cols:list):
        for _col in cols:
            df = df.withColumn(_col, F.col(_col).cast(DoubleType()))
        return df

    def _to_int(df, cols:list):
        for _col in cols:
            df = df.withColumn(_col, F.col(_col).cast(IntegerType()))
        return df

    def _to_timestamp(df, cols:list):
        for _col in cols:
            df = df.withColumn(_col, F.col(_col).cast(TimestampType()))
        return df

    def _fill_null(df, cols:list, filled_value):
        for _col in cols:
            df = df.fillna({_col: filled_value})
        return df

    def _trim(df, cols:list):
        for _col in cols:
            df = df.withColumn(_col, F.trim(F.col(_col)))
        return df

    def _clean_boolean_YN(df, _col:str):
        invalid_df = df.filter(((F.col(_col) != 'Y') & (F.col(_col) != 'N')) | (F.col(_col).isNull()))
        df = df.filter((F.col(_col) == 'Y') | (F.col(_col) == 'N'))
        invalid_df = invalid_df.withColumn('Invalid', F.lit('store_and_fwd_flag'))
        return df, invalid_df
    
    def Cleansing():
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

        yt_df = spark.read.parquet("s3a://airflow-spark-etl/Raw-zone/yellow-taxi/202401/*.parquet")

        string_cols = ['VendorID', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'payment_type']
        double_cols = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Airport_fee']
        int_cols = ['passenger_count']
        timestamp_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

        yt_df = _to_string(yt_df, string_cols)
        yt_df = _to_double(yt_df, double_cols)
        yt_df = _to_int(yt_df, int_cols)
        yt_df = _to_timestamp(yt_df, timestamp_cols)
        yt_df = _fill_null(yt_df, double_cols, 0)
        yt_df = _trim(yt_df, string_cols)
        yt_df, invalid_flag_df = _clean_boolean_YN(yt_df, 'store_and_fwd_flag')

        yt_dup_df = yt_df.groupBy(yt_df.columns).count().filter(F.col('count') > 1)
        yt_dup_df = yt_dup_df.drop('count')
        yt_dup_df = yt_dup_df.withColumn('Invalid', F.lit('duplicate records'))
        yt_dup_cnt = yt_dup_df.count()
        if yt_dup_cnt > 0:
            yt_df = yt_df.dropDuplicates()
        else:
            print('no duplicate records')

        yt_df.show()

        invalid_dfs = invalid_flag_df.union(yt_dup_df)
        invalid_dfs.show()

        if invalid_dfs.count() > 0:
            invalid_dfs.coalesce(1).write.mode("overwrite").parquet('s3a://airflow-spark-etl/Invalid/yellow-taxi/202401/')
            print('write inconsistent data to Invalid')

        yt_df.coalesce(1).write.mode("overwrite").parquet('s3a://airflow-spark-etl/Clean-zone/yellow-taxi/202401/')

    Cleansing()
    os.system('kill %d' % os.getpid())