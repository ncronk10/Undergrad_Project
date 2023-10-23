# Databricks notebook source
# DBTITLE 1,Players Table
players = (spark.table('default.players')
           .withColumnRenamed('_id','player_id')
           .dropna())
#display(players)

# COMMAND ----------

# DBTITLE 1,Salaries Table
salaries = (spark.table('default.salaries')
            .dropna())
#display(salaries)
