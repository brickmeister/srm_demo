# Databricks notebook source

"""
Configuration for demo
"""

# unity catalog
catalog = "SRM"
# bronze database
bronze = "bronze"
# silver database
silver = "silver"
# gold database
gold = "gold"

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --
# MAGIC -- Turn on change data feeds
# MAGIC --
# MAGIC
# MAGIC -- set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;

# COMMAND ----------


"""
Programmatic values [DON'T CHANGE]
"""

# easy export as array
databases = [bronze, silver, gold]
