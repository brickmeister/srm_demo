# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Clean raw supplier data

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
# MAGIC Silver dataframe
# MAGIC """
# MAGIC
# MAGIC _silver_df = spark.read.format("delta").table(f"{catalog}.{silver}.osa_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add flags

# COMMAND ----------

display(_silver_df)

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Flag some problematic values
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import col, expr
# MAGIC
# MAGIC
# MAGIC
# MAGIC _final_cut = (_silver_df.withColumn("FLAG_ON_HAND_INVENTORY_UNITS", expr("CASE WHEN (ON_HAND_INVENTORY_UNITS + REPLENISHMENT_UNITS) < total_sales_units THEN 1 ELSE 0 END"))
# MAGIC                         .withColumn("FLAG_SHELF_CAPACITY", expr("CASE WHEN ON_HAND_INVENTORY_UNITS + REPLENISHMENT_UNITS > SHELF_CAPACITY THEN 1 ELSE 0 END"))
# MAGIC )
# MAGIC
# MAGIC display(_final_cut)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to delta

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Write out data to delta gold table
# MAGIC """
# MAGIC
# MAGIC from datetime import datetime
# MAGIC
# MAGIC try:
# MAGIC   (_final_cut.write.format("delta")
# MAGIC              .mode("overwrite")
# MAGIC              .option("overwriteSchema", True)
# MAGIC              .saveAsTable(f"{catalog}.{gold}.osa_flagged"))
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"{str(datetime.now())} : Failed to write out flagged table, err : {err}")
