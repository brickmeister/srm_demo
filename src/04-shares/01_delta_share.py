# Databricks notebook source
# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Create delta shares
# MAGIC """
# MAGIC
# MAGIC spark.sql(f"CREATE SHARE IF NOT EXISTS SRM_OSA_SHARE;")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC
# MAGIC """
# MAGIC
# MAGIC for _table in ["srm.silver.osa_clean", "srm.silver.vendor_leadtime_clean"]:
# MAGIC   spark.sql(f"ALTER SHARE SRM_OSA_SHARE ADD TABLE {_table}")
