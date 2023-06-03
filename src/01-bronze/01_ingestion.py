# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Ingest raw supplier data into a Bronze layer lakehouse

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Setup global values used throughout this notebook
# MAGIC """
# MAGIC
# MAGIC # keep track of all files that are downloaded
# MAGIC ingest_dict = dict()
# MAGIC delta_tables = dict()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Get data
# MAGIC
# MAGIC The data used in this demo is currently retrieved from Tredence repositories and is provided as is.
# MAGIC
# MAGIC * Source : https://github.com/tredenceofficial/OSA-Data/

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Download data and store in Databricks
# MAGIC """
# MAGIC
# MAGIC import os
# MAGIC import urllib.request
# MAGIC from datetime import datetime
# MAGIC
# MAGIC # source file
# MAGIC _raw_files = ["https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/osa_raw_data.csv",
# MAGIC               "https://raw.githubusercontent.com/tredenceofficial/OSA-Data/main/vendor_leadtime_info.csv",
# MAGIC               "https://raw.githubusercontent.com/brickmeister/srm_demo/main/data/dim_sku.csv"]
# MAGIC
# MAGIC # retrieve files
# MAGIC for _raw_file in _raw_files:
# MAGIC   try:
# MAGIC     with urllib.request.urlopen(_raw_file) as _f:
# MAGIC       _out_file = f"/dbfs/tmp/{_raw_file.split('/')[-1]}"
# MAGIC       with open(_out_file, 'wb') as _w:
# MAGIC         # write out the csv file
# MAGIC         _w.write(_f.read())
# MAGIC         # replace the fuse url with a dbfs:/ url and track
# MAGIC         ingest_dict[_out_file.replace("/dbfs/", "dbfs:/")] = None
# MAGIC   except Exception as err:
# MAGIC     raise ValueError(f"Failed to ingest {_raw_file}, err : {err}")
# MAGIC else:
# MAGIC   print(f"{str(datetime.now())} : Downloaded needed datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Ingest data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read Data

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Create the data frames that will read data
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import col, to_timestamp
# MAGIC from pyspark.sql.types import StringType
# MAGIC
# MAGIC for _out_file in ingest_dict:
# MAGIC   try:
# MAGIC     if _out_file == "dbfs:/tmp/osa_raw_data.csv":
# MAGIC       _df = (spark.read.format("csv")
# MAGIC                   .option("header", True)
# MAGIC                   .option("inferSchema", True)
# MAGIC                   .load(_out_file)
# MAGIC                   .withColumn("date", to_timestamp(col("date").cast(StringType()), "yyyyMMdd"))
# MAGIC                   .withColumn("supplier_id", col("store_id"))
# MAGIC                   .drop(col("store_id")))
# MAGIC     elif _out_file == "dbfs:/tmp/dim_sku.csv":
# MAGIC       _df = (spark.read.format("csv")
# MAGIC                   .option("header", True)
# MAGIC                   .option("inferSchema", True)
# MAGIC                   .load(_out_file)
# MAGIC     else:
# MAGIC       _df = (spark.read.format("csv")
# MAGIC             .option("header", True)
# MAGIC             .option("inferSchema", True)
# MAGIC             .load(_out_file)
# MAGIC             .withColumn("supplier_id", col("store_id"))
# MAGIC             .drop(col("store_id")))
# MAGIC     
# MAGIC     ingest_dict[_out_file] = _df
# MAGIC     # register the dataframe as a temporary view for access
# MAGIC   except Exception as err:
# MAGIC     raise ValueError(f"Failed to read {_out_file}, err : {err}")
# MAGIC else:
# MAGIC   print(f"{str(datetime.now())} : Read raw data")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to delta

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Write to delta DataWarehouse
# MAGIC """
# MAGIC
# MAGIC for _out_file, _df in ingest_dict.items():
# MAGIC   try:
# MAGIC     _table_name = f"{catalog}.{bronze}.{_out_file.split('/')[-1].split('.csv')[0]}"
# MAGIC     (_df.write.format("delta")
# MAGIC                # overwrite for demo purposes
# MAGIC               .mode("overwrite")
# MAGIC               .option("overwriteSchema", True)
# MAGIC               .saveAsTable(_table_name))
# MAGIC     delta_tables[_out_file] = _table_name
# MAGIC   except Exception as err:
# MAGIC     raise ValueError(f"{str(datetime.now())} : Failed to write to delta table : {_table_name} from {_out_file}, err : {err}")
# MAGIC else:
# MAGIC   print(f"{str(datetime.now())} : Wrote to bronze delta")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Integrity Checks

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Check if files are loaded
# MAGIC """
# MAGIC
# MAGIC for _out_file, _delta_table in delta_tables.items():
# MAGIC   # get the dataframe for a delta table
# MAGIC   _df = spark.read.format("delta").table(_delta_table)
# MAGIC   # check if count is greater than 0
# MAGIC   assert(_df.count() > 0)
# MAGIC else:
# MAGIC   print(f"{str(datetime.now())} : Checked bronze delta tables")
