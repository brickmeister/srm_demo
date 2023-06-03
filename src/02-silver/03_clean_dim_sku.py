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
# MAGIC Bronze dataframe
# MAGIC """
# MAGIC
# MAGIC _bronze_df = spark.read.format("delta").table(f"{catalog}.{bronze}.dim_sku").distinct()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Create linkages for alternate SKUs from different suppliers
# MAGIC
# MAGIC The DIM_SKU dataset contains mappings between drugs and suppliers. In order to suppy orders when suppliers are unable to fulfill requested quantities, we need to develop a dataset that provides alternate mappings to SKUs for the same biologics from different suppliers.

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Create the graph network between SKUs and alternative SKus
# MAGIC
# MAGIC Note : edge : alternative sku
# MAGIC        equivalent_quantity : equivalent dose 
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import col, explode, collect_list
# MAGIC
# MAGIC _data_df = (_bronze_df.groupBy(col("generic_name"), col("unit"), col("biologic_delivery"))
# MAGIC                       .agg(collect_list(col("sku")).alias("edges"))
# MAGIC                       .withColumn("edge", explode(col("edges")))
# MAGIC                       .drop(col("edges")))
# MAGIC
# MAGIC _vertices_df = (_bronze_df)
# MAGIC
# MAGIC _edges_df = (_data_df.select(col("generic_name"), col("edge")))
# MAGIC
# MAGIC _resolved_df = (_data_df.join(_bronze_df, ["generic_name", "unit", "biologic_delivery"],
# MAGIC                               how = "inner")
# MAGIC                         .join(_bronze_df.select(col("sku").alias("edge"), col("quantity").alias("edge_quantity")),
# MAGIC                               ["edge"],
# MAGIC                               how = "inner")
# MAGIC                         .withColumn("equivalent_quantity", col("quantity") / col("edge_quantity"))
# MAGIC                         .drop("edge_quantity"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Final cut

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Display the final dataset
# MAGIC """
# MAGIC
# MAGIC display(_resolved_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to delta

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Write out networked DIM_SKU data
# MAGIC """
# MAGIC
# MAGIC try:
# MAGIC   (_resolved_df.write.format("delta")
# MAGIC                     .mode("append")
# MAGIC                     .saveAsTable(f"{catalog}.{silver}.DIM_SKU_EDGE"))
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"{str(datetime.now())} : Failed to write out networked table, err : {err}")
