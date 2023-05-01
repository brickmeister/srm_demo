# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Configuration for demo
# MAGIC """
# MAGIC
# MAGIC # unity catalog
# MAGIC catalog = "SRM"
# MAGIC # bronze database
# MAGIC bronze = "bronze"
# MAGIC # silver database
# MAGIC silver = "silver"
# MAGIC # gold database
# MAGIC gold = "gold"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Turn on change data feeds
# MAGIC --
# MAGIC
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC """
# MAGIC Programmatic values [DON'T CHANGE]
# MAGIC """
# MAGIC
# MAGIC # easy export as array
# MAGIC databases = [bronze, silver, gold]
