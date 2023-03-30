#!/usr/bin/env python

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType, DoubleType
from pyspark.sql.functions import col, lit, array, explode, arrays_zip, posexplode,     sum as sum_, max as max_, min as min_, year, month

# === local spark settings ===
# import os
# os.environ["AWS_PROFILE"] = "service_wp"

# # set spark configuration
# conf = SparkConf() \
#     .setAppName('process-weather-stations') \
#     .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2') \
#     .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
#     .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.profile.ProfileCredentialsProvider')

# # instantiate Spark session
# spark = SparkSession.builder.config(conf=conf).getOrCreate()
# === ===

# === aws emr settings ===
spark = SparkSession.builder.appName('process-weather-stations').getOrCreate()

# === ===

# define schema and read parquet files
station_schema = StructType([StructField('STATION', StringType(), True),
 StructField('NAME', StringType(), True),
 StructField('LATITUDE', DoubleType(), True),
 StructField('LONGITUDE', DoubleType(), True),
 StructField('ELEVATION', DoubleType(), True),
 StructField('DATE', DateType(), True),
 StructField('HR00Val', LongType(), True),
 StructField('HR00MF', StringType(), True),
 StructField('HR00QF', StringType(), True),
 StructField('HR00S1', StringType(), True),
 StructField('HR00S2', StringType(), True),
 StructField('HR01Val', LongType(), True),
 StructField('HR01MF', StringType(), True),
 StructField('HR01QF', StringType(), True),
 StructField('HR01S1', StringType(), True),
 StructField('HR01S2', StringType(), True),
 StructField('HR02Val', LongType(), True),
 StructField('HR02MF', StringType(), True),
 StructField('HR02QF', StringType(), True),
 StructField('HR02S1', StringType(), True),
 StructField('HR02S2', StringType(), True),
 StructField('HR03Val', LongType(), True),
 StructField('HR03MF', StringType(), True),
 StructField('HR03QF', StringType(), True),
 StructField('HR03S1', StringType(), True),
 StructField('HR03S2', StringType(), True),
 StructField('HR04Val', LongType(), True),
 StructField('HR04MF', StringType(), True),
 StructField('HR04QF', StringType(), True),
 StructField('HR04S1', StringType(), True),
 StructField('HR04S2', StringType(), True),
 StructField('HR05Val', LongType(), True),
 StructField('HR05MF', StringType(), True),
 StructField('HR05QF', StringType(), True),
 StructField('HR05S1', StringType(), True),
 StructField('HR05S2', StringType(), True),
 StructField('HR06Val', LongType(), True),
 StructField('HR06MF', StringType(), True),
 StructField('HR06QF', StringType(), True),
 StructField('HR06S1', StringType(), True),
 StructField('HR06S2', StringType(), True),
 StructField('HR07Val', LongType(), True),
 StructField('HR07MF', StringType(), True),
 StructField('HR07QF', StringType(), True),
 StructField('HR07S1', StringType(), True),
 StructField('HR07S2', StringType(), True),
 StructField('HR08Val', LongType(), True),
 StructField('HR08MF', StringType(), True),
 StructField('HR08QF', StringType(), True),
 StructField('HR08S1', StringType(), True),
 StructField('HR08S2', StringType(), True),
 StructField('HR09Val', LongType(), True),
 StructField('HR09MF', StringType(), True),
 StructField('HR09QF', StringType(), True),
 StructField('HR09S1', StringType(), True),
 StructField('HR09S2', StringType(), True),
 StructField('HR10Val', LongType(), True),
 StructField('HR10MF', StringType(), True),
 StructField('HR10QF', StringType(), True),
 StructField('HR10S1', StringType(), True),
 StructField('HR10S2', StringType(), True),
 StructField('HR11Val', LongType(), True),
 StructField('HR11MF', StringType(), True),
 StructField('HR11QF', StringType(), True),
 StructField('HR11S1', StringType(), True),
 StructField('HR11S2', StringType(), True),
 StructField('HR12Val', LongType(), True),
 StructField('HR12MF', StringType(), True),
 StructField('HR12QF', StringType(), True),
 StructField('HR12S1', StringType(), True),
 StructField('HR12S2', StringType(), True),
 StructField('HR13Val', LongType(), True),
 StructField('HR13MF', StringType(), True),
 StructField('HR13QF', StringType(), True),
 StructField('HR13S1', StringType(), True),
 StructField('HR13S2', StringType(), True),
 StructField('HR14Val', LongType(), True),
 StructField('HR14MF', StringType(), True),
 StructField('HR14QF', StringType(), True),
 StructField('HR14S1', StringType(), True),
 StructField('HR14S2', StringType(), True),
 StructField('HR15Val', LongType(), True),
 StructField('HR15MF', StringType(), True),
 StructField('HR15QF', StringType(), True),
 StructField('HR15S1', StringType(), True),
 StructField('HR15S2', StringType(), True),
 StructField('HR16Val', LongType(), True),
 StructField('HR16MF', StringType(), True),
 StructField('HR16QF', StringType(), True),
 StructField('HR16S1', StringType(), True),
 StructField('HR16S2', StringType(), True),
 StructField('HR17Val', LongType(), True),
 StructField('HR17MF', StringType(), True),
 StructField('HR17QF', StringType(), True),
 StructField('HR17S1', StringType(), True),
 StructField('HR17S2', StringType(), True),
 StructField('HR18Val', LongType(), True),
 StructField('HR18MF', StringType(), True),
 StructField('HR18QF', StringType(), True),
 StructField('HR18S1', StringType(), True),
 StructField('HR18S2', StringType(), True),
 StructField('HR19Val', LongType(), True),
 StructField('HR19MF', StringType(), True),
 StructField('HR19QF', StringType(), True),
 StructField('HR19S1', StringType(), True),
 StructField('HR19S2', StringType(), True),
 StructField('HR20Val', LongType(), True),
 StructField('HR20MF', StringType(), True),
 StructField('HR20QF', StringType(), True),
 StructField('HR20S1', StringType(), True),
 StructField('HR20S2', StringType(), True),
 StructField('HR21Val', LongType(), True),
 StructField('HR21MF', StringType(), True),
 StructField('HR21QF', StringType(), True),
 StructField('HR21S1', StringType(), True),
 StructField('HR21S2', StringType(), True),
 StructField('HR22Val', LongType(), True),
 StructField('HR22MF', StringType(), True),
 StructField('HR22QF', StringType(), True),
 StructField('HR22S1', StringType(), True),
 StructField('HR22S2', StringType(), True),
 StructField('HR23Val', LongType(), True),
 StructField('HR23MF', StringType(), True),
 StructField('HR23QF', StringType(), True),
 StructField('HR23S1', StringType(), True),
 StructField('HR23S2', StringType(), True),
 StructField('DlySum', LongType(), True),
 StructField('DlySumMF', StringType(), True),
 StructField('DlySumQF', StringType(), True),
 StructField('DlySumS1', StringType(), True),
 StructField('DlySumS2', StringType(), True)])

df = spark.read.schema(station_schema).parquet('s3a://weather-data-kpde/raw/*')

# function to generate hourly columns
def generate_cols_as_array(col_name):
    return array([col(f'HR{i:02}{col_name}') for i in range(24)]).alias(col_name)

# unstack hourly columns
col_types = ['Val', 'MF', 'QF', 'S1', 'S2']

df_out = (df.select('STATION', 'DATE', arrays_zip(*[generate_cols_as_array(c) for c in col_types]
                                          ).alias('zip'))  # generate zipped arrays for each hour
 .select('*', posexplode('zip').alias('hr', 'exp'))  # explode zipped arrays
 .select('*', *[col('exp')[c].alias(c) for c in col_types],  # extract array
        )
 .select('STATION', 'DATE', 'hr', *col_types)
)

# replace -9999 values and ' ' strings with null
df_out = (df_out.replace({-9999: None}, subset=['Val'])
     .replace({' ': None}, subset=['MF', 'QF', 'S1', 'S2'])
)

# create year column
df_out = df_out.withColumn('year', year('DATE'))

# write parquet files
df_out.coalesce(100).write.mode('overwrite').parquet('s3a://weather-data-kpde/out/precipitation')

spark.stop()
