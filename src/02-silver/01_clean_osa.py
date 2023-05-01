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
# MAGIC _bronze_df = spark.read.format("delta").table(f"{catalog}.{bronze}.osa_raw_data")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Check for censored data (missing records)
# MAGIC
# MAGIC The inventory data contains records for products in specific stores when an inventory-related transaction occurs. Since not every product *moves* on every date, there will be days for which there is no data for certain store and product SKU combinations. 
# MAGIC
# MAGIC Time series analysis techniques used in our framework require a complete set of records for products within a given location. To address the *missing* entries, we will generate a list of all dates for which we expect records. A cross-join with store-SKU combinations will provide the base set of records for which we expect data.  
# MAGIC
# MAGIC In the real world, not all products are intended to be sold at each location at all times.  In an analysis of non-simulated data, we may require additional information to determine the complete set of dates for a given store-SKU combination for which we should have data:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Generate time series

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Generate a time series dataframe for dates
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import min, max, lit, col, sequence, expr, explode
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC
# MAGIC # get start date of data set
# MAGIC # should leverage delta column min/max stats
# MAGIC _date_series = (_bronze_df.select(min(col("date")).alias("min_date"),
# MAGIC                                   max(col("date")).alias("max_date"))
# MAGIC                           .select(sequence(col("min_date"), col("max_date"), expr("Interval 1 day")).alias("dates"))
# MAGIC                           .select(explode(col("dates")).alias("date")))
# MAGIC
# MAGIC display(_date_series)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Store-SKU Combinations

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Generate a dataframe of all store-sku combinations
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC _store_skus = (_bronze_df.select(col("supplier_id"),
# MAGIC                                  col("sku"), 
# MAGIC                                  col("product_category"))
# MAGIC                          .distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Censored time series

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Generate time series inventory data
# MAGIC """
# MAGIC
# MAGIC _censored_data = (_date_series.crossJoin(_store_skus)
# MAGIC                               .join(_bronze_df, on = ["date", "product_category", "supplier_id", "sku"], how = "left"))
# MAGIC
# MAGIC display(_censored_data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Censored count

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Get censored data count
# MAGIC """
# MAGIC
# MAGIC display(_censored_data.where(col("total_sales_units").isNull()).count())

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC We now have one record for each date-store-SKU combination in our dataset.  However, on those dates for which there were no inventory changes, we are currently missing information about the inventory status of those stores and SKUs.  To address this, we will employ a combination of forward filling, *i.e.* applying the last valid record to subsequent records until a new value is encountered, and defaults.  For the forward fill, we will make use of the [last()](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.functions.last.html) function, providing a value of *True* for the *ignorenulls* argument which will force it to retrieve the last non-null value in a sequence:

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Imputed time series

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Impute values for censored data
# MAGIC """
# MAGIC
# MAGIC from pyspark.sql.functions import last, col
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC _imputed_data = (_censored_data.withColumn("total_sales_units", 
# MAGIC                                           last(col("total_sales_units"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("on_hand_inventory_units", 
# MAGIC                                           last(col("on_hand_inventory_units"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("replenishment_units", 
# MAGIC                                           last(col("replenishment_units"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("inventory_pipeline", 
# MAGIC                                           last(col("inventory_pipeline"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("units_in_transit", 
# MAGIC                                           last(col("units_in_transit"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("units_in_dc", 
# MAGIC                                           last(col("units_in_dc"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("units_on_order", 
# MAGIC                                           last(col("units_on_order"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("units_under_promotion", 
# MAGIC                                           last(col("units_under_promotion"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date")))
# MAGIC                               .withColumn("shelf_capacity", 
# MAGIC                                           last(col("shelf_capacity"), True)
# MAGIC                                               .over(Window.partitionBy(col("supplier_id"), 
# MAGIC                                                                        col("product_category"), 
# MAGIC                                                                        col("sku"))
# MAGIC                                                           .orderBy("date"))))
# MAGIC display(_imputed_data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Censored count

# COMMAND ----------

display(_imputed_data.where(col("total_sales_units").isNull()).count())

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
# MAGIC _final_cut = _imputed_data.fillna(-1)
# MAGIC
# MAGIC display(_final_cut)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Censored count

# COMMAND ----------

display(_final_cut.where(col("total_sales_units").isNull()).count())

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
# MAGIC              .saveAsTable(f"{catalog}.{silver}.osa_clean"))
# MAGIC except Exception as err:
# MAGIC   raise ValueError(f"{str(datetime.now())} : Failed to write out cleaned table, err : {err}")
