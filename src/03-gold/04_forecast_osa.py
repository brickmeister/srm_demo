# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Forecast Inventory

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %run ../00-setup/00_config

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prepare data

# COMMAND ----------


"""
Read in OSA data
"""

# Columns to train against
columns = ["product_category", "supplier_id", "sku", "date", "total_sales_units", "replenishment_units", "inventory_pipeline", "units_in_transit", "units_in_dc", "units_on_order", "units_under_promotion", "shelf_capacity", "on_hand_inventory_units"]

# Load dataset
train_df = spark.read.format("delta").table(f"{catalog}.{silver}.osa_clean").select(*columns)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## AutoML

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Train Model

# COMMAND ----------


"""
Forecast inventory units using AutoML
"""

from databricks import automl

try:
  # Run AutoML
  summary = automl.regress(train_df, target_col="on_hand_inventory_units", timeout_minutes=5)
except Exception as error:
  raise ValueError(f"Failed to train classifier, error : {error}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Forecast with Model

# COMMAND ----------


"""
Predict next ADT code with best model
"""

import mlflow
from pyspark.sql.functions import round

# get the model URI for the best model
model_uri = summary.best_trial.model_path

# get some ADT messages
df = spark.read.format("delta").table(f"{catalog}.{silver}.osa_clean")

try:
  # setup the pyfunc udf for prediction
  predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type="string")
except Exception as err:
  raise ValueError(f"Failed to load model for inferencing, model_uri : {model_uri}, error : {err}")

# predict the results
predict_df = df.withColumn("predicted_on_hand_inventory_units", round(predict_udf(), 0))

try:
  # display the results
  display(predict_df.select("date", "product_category", "sku", "predicted_on_hand_inventory_units"))
except Exception as err:
  raise ValueError(f"Failed to inference with model : {model_uri}, error : {err}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Write to delta

# COMMAND ----------


"""
Write predicted results to Delta Lake
"""

predict_df.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{gold}.osa_forecasted")

# COMMAND ----------


