# Databricks notebook source
# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Create delta shares
# MAGIC """
# MAGIC
# MAGIC spark.sql(f"CREATE SHARE IF NOT EXISTS SRM_OSA_SHARE")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Add tables to share
# MAGIC """
# MAGIC
# MAGIC for _table in [f"{catalog}.{silver}.osa_clean", f"{catalog}.{silver}.vendor_leadtime_clean", f"{catalog}.{silver}.dim_sku_edge",
# MAGIC                f"{catalog}.{gold}.osa_flagged", f"{catalog}.{gold}.sku_flagged", f"{catalog}.{gold}.supplier_flagged"]:
# MAGIC   spark.sql(f"ALTER SHARE SRM_OSA_SHARE ADD TABLE {_table}")
