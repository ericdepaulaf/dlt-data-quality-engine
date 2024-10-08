# Databricks notebook source
# This cell installs the dlt_data_quality_lib library from a wheel file located in the workspace.
%pip install /Workspace/Users/rodrigo.improta@databricks.com/dlt_data_quality_lib/dlt_data_quality_lib-1.0-py2.py3-none-any.whl

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
# Importing dlt_data_quality_lib to access custom data quality functions provided by the installed library
from dlt_data_quality_lib.dlt_data_quality_lib import *

# COMMAND ----------

# This cell loads predefined data quality rules for the specified DLT pipeline.
load_data_quality_rules(spark=spark, catalog="rimprota_demo", schema="dlt_data_quality_lib")

# COMMAND ----------

# DBTITLE 0,Example table 1
@dlt.table(name="bronze_trips")
@data_quality(table="bronze_trips")
def table():
  return spark.readStream.table("rimprota_demo.nyc_taxi.trips")

# COMMAND ----------

@dlt.table(name="silver_trips")
@data_quality(table="silver_trips")
def table():
  return dlt.readStream("bronze_trips")

# COMMAND ----------

@dlt.table(name="gold_agg_dropoff_zip_code")
@data_quality(table="gold_agg_dropoff_zip_code")
def table():
  return (
    dlt.read('silver_trips')
      .groupBy(col('dropoff_zip'))
      .agg(avg("fare_amount").alias("avg_fare"),
           avg("trip_distance").alias("avg_distance"))
  )
