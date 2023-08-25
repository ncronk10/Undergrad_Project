# Databricks notebook source
# DBTITLE 1,Players Table
players = spark.table('default.players').withColumnRenamed('_id','player_id')
#display(players)

# COMMAND ----------

# DBTITLE 1,Salaries Table
salaries = spark.table('default.salaries')
#display(salaries)

# COMMAND ----------

# DBTITLE 1,Master Table
master = players.join(salaries,['player_id'], how='left')
#display(master)
