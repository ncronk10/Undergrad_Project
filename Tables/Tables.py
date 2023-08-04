# Databricks notebook source
# DBTITLE 1,Players Table
players = spark.table('default.players')
#display(players)

# COMMAND ----------

# DBTITLE 1,Salaries Table
salaries = spark.table('default.salaries')
#display(salaries)
