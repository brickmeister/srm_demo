# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Clean raw vendor data

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
# MAGIC Bronze dataframe
# MAGIC """
# MAGIC
# MAGIC _bronze_df = spark.read.format("delta").table(f"{catalog}.{bronze}.vendor_leadtime_info")

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Get counts of any missing data
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC display(_bronze_df.where(col("supplier_id").isNull()).count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Clean vendor leadtime dataset

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Initial row count

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Get row counts of non-deduplicated data
# MAGIC """
# MAGIC
# MAGIC display(_bronze_df.count())

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Generate a dataframe of all store-sku combinations
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC _deduped = _bronze_df.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Deduplicated count

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Get censored data count
# MAGIC """
# MAGIC
# MAGIC display(_deduped.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Final cut

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Fill in left over values with zero from imputed data
# MAGIC """
# MAGIC
# MAGIC _final_cut = _deduped.fillna(-1)
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
# MAGIC Write out data to delta silver table
# MAGIC """
# MAGIC
# MAGIC from datetime import datetime
# MAGIC
# MAGIC try:
# MAGIC   (_final_cut.write.format("delta")
# MAGIC              .mode("overwrite")
# MAGIC              .option("overwriteSchema", True)
# MAGIC              .saveAsTable(f"{catalog}.{silver}.vendor_leadtime_clean"))
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"{str(datetime.now())} : Failed to write out cleaned table, err : {err}")
