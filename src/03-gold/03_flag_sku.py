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
# MAGIC Gold dataframe
# MAGIC """
# MAGIC
# MAGIC _gold_df = spark.read.format("delta").table(f"{catalog}.{gold}.osa_flagged")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Add flags

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC
# MAGIC """
# MAGIC Flag suppliers risk
# MAGIC """
# MAGIC
# MAGIC _final_cut = spark.sql("""
# MAGIC         select sku,
# MAGIC               case when avg(FLAG_ON_HAND_INVENTORY_UNITS) >= 0.5
# MAGIC                     then 1
# MAGIC                     else 0
# MAGIC                     end as FLAG_INVENTORY_RISK,
# MAGIC               case when avg(FLAG_SHELF_CAPACITY) >= 0.5
# MAGIC                     then 1
# MAGIC                     else 0
# MAGIC                     end as FLAG_SHELF_RISK
# MAGIC         from srm.gold.osa_flagged
# MAGIC         group by sku;
# MAGIC """)
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
# MAGIC              .saveAsTable(f"{catalog}.{gold}.sku_flagged"))
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"{str(datetime.now())} : Failed to write out flagged table, err : {err}")

# COMMAND ----------


