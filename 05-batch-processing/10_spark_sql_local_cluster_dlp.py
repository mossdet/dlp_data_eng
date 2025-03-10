import argparse
import pyspark
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--input_green', required=True)
parser.add_argument('--input_yellow', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_green = args.input_green
input_yellow = args.input_yellow
output = args.output


# spark = SparkSession.builder \
#     .master("spark://instance-20250227-050901.us-west3-c.c.dateng-dlp-05.internal:7077") \
#     .appName('test') \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()


# Read green taxi data
df_green = spark.read.parquet(input_green)
df_green.printSchema()

# Read yellow taxi data
df_yellow = spark.read.parquet(input_yellow)
df_yellow.printSchema()


# ### Generate dataframe containing datat from both green and yellow.<br> You need to check which fields are in common and which are not.
df_green = df_green \
    .withColumnRenamed('lpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime', 'dropoff_datetime')

df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

common_colums = []
#yellow_columns = set(df_yellow.columns)
for col in set(df_green.columns):
    if col in set(df_yellow.columns):
        common_colums.append(col)
print(common_colums)


# Another option is the following, but it does not preserve the order of the columns
set(df_yellow.columns) & set(df_green.columns)


# ### Select fields in common and add field about the service type (green or yellow taxis)
df_green_sel = df_green.select(common_colums).withColumn('service_type', F.lit('green'))
df_yellow_sel = df_yellow.select(common_colums).withColumn('service_type', F.lit('yellow'))

# ### Combine both dataframes
df_trips_data = df_green_sel.unionAll(df_yellow_sel)

df_trips_data.groupBy('service_type').count().show()

# # Use SQL within SPark
# Tell spark that the created dataframe is a table
df_trips_data.createOrReplaceTempView('trips_data')

spark.sql("""
SELECT
    service_type,
    count(1)
FROM
    trips_data
GROUP BY 
    service_type
""").show()

df_result = spark.sql("""
SELECT 
    -- Revenue grouping 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month, 
    service_type, 

    -- Revenue calculation 
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,

    -- Additional calculations
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM
    trips_data
GROUP BY
    1, 2, 3
""")

df_result.show()

# The resulting table can now be saved
# One option would be: df_result.write.parquet('../data/report/revenue/', mode='overwrite')
# But this command creates several files

# Using the coalesce option, we can save the data in as many files as we specify, e.g. 1 partition would be coalesce(1)
df_result.coalesce(1).write.parquet(output, mode='overwrite')

