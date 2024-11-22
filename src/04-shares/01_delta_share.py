# Databricks notebook source
# MAGIC %run ../00-setup/00_config

# COMMAND ----------


"""
Create delta shares
"""

spark.sql(f"CREATE SHARE IF NOT EXISTS SRM_OSA_SHARE")

# COMMAND ----------


"""
Add tables to share
"""

for _table in [f"{catalog}.{silver}.osa_clean", f"{catalog}.{silver}.vendor_leadtime_clean", f"{catalog}.{silver}.dim_sku_edge",
               f"{catalog}.{gold}.osa_flagged", f"{catalog}.{gold}.sku_flagged", f"{catalog}.{gold}.supplier_flagged"]:
  spark.sql(f"ALTER SHARE SRM_OSA_SHARE ADD TABLE {_table}")
