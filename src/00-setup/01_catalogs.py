# Databricks notebook source
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Create Datawarehouse
# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Create some catalogs
# MAGIC """
# MAGIC
# MAGIC try:
# MAGIC   spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"Failed to create {catalog}, err : {err}")
# MAGIC
# MAGIC for _database in databases:
# MAGIC   try:
# MAGIC     spark.sql(f"CREATE DATABASE If NOT EXISTS {catalog}.{_database}")
# MAGIC   except Exception as err:
# MAGIC     raise ValueError(f"Failed to create {catalog}.{_database}, err : {err}")
