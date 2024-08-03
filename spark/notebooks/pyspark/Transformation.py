# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, DateType, DoubleType, LongType
from pyspark.sql import Window
from datetime import date
import os

if __name__ == '__main__':

    def _add_primark_key(df):
        window_spec = Window.orderBy(F.lit(1))
        df = df.withColumn('ID', F.row_number().over(window_spec))
        df = df.withColumn('TripID', F.concat(F.lit('YellowTaxi'), F.format_string('%09d', F.col('ID')))).drop('ID')
        df_cols = df.columns
        df_cols = [df_cols[-1]] + df_cols[:-1]
        df = df.select(df_cols)
        return df

    def _cal_trip_dur(df):
        df = df.withColumn('trip_duration_in_min',
            F.round((F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 60)
        )
        df = df.withColumn('trip_duration_in_min', F.col('trip_duration_in_min').cast(IntegerType()))
        return df

    def _cal_monthly_amount(df):
        window_spec = Window.partitionBy('VendorID', F.year('tpep_pickup_datetime'), F.month('tpep_pickup_datetime'))
        df = df.withColumn('monthly_amount', F.round(F.sum('total_amount').over(window_spec), 2))
        df = df.orderBy('TripID')
        return df

    def _convert_payment_type(df):
        df = df.withColumn('payment_type',
        F.when(df['payment_type'] == '1', 'Cash')
            .when(df['payment_type'] == '2', 'Credit Card')
            .when(df['payment_type'] == '3', 'Payme')
            .when(df['payment_type'] == '4', 'Octopus')
            .otherwise('Unknown')
        )
        return df

    def _add_total_amount_is_positive(df):
        df = df.withColumn('total_amount_is_positive',
        F.when(df['total_amount'] > 0, 'Y')
            .otherwise('N')
        )
        return df

    def _get_aggregated_df(df):
        # Group by VendorID, year, and month
        df = df.groupBy(
            'VendorID',
            F.year('tpep_pickup_datetime').alias("pickup_year"),
            F.month('tpep_pickup_datetime').alias("pickup_month")
        ).agg(
            F.sum('passenger_count').alias('total_passenger_count'),
            F.max('trip_distance').alias('max_trip_distance'),
            F.min('trip_distance').alias('min_trip_distance'),
            F.max('trip_duration_in_min').alias('max_trip_duration_in_min'),
            F.min('trip_duration_in_min').alias('min_trip_duration_in_min')
        )
        df = df.orderBy('VendorID', 'pickup_year', 'pickup_month')
        
        return df
    
    def Transformation():
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

        yt_df = spark.read.parquet("s3a://airflow-spark-etl/Clean-zone/yellow-taxi/202401/*.parquet")

        yt_df = _add_primark_key(yt_df)
        yt_df = _cal_trip_dur(yt_df)
        yt_df = _cal_monthly_amount(yt_df)
        yt_df = _convert_payment_type(yt_df)
        yt_df = _add_total_amount_is_positive(yt_df)
        
        yt_df.coalesce(1).write.mode("overwrite").parquet('s3a://airflow-spark-etl/Curated-zone/yellow-taxi/transformed/202401/')
        
        agg_df = _get_aggregated_df(yt_df)
        agg_df.coalesce(1).write.mode("overwrite").parquet('s3a://airflow-spark-etl/Curated-zone/yellow-taxi/aggregated/202401/')

    Transformation()
    os.system('kill %d' % os.getpid())