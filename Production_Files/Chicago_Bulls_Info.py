# Databricks notebook source
# MAGIC %md
# MAGIC #### Chicago Bulls Players' Performance Requirements
# MAGIC 1. Years
# MAGIC 2. Total Money Spent Per Draft

# COMMAND ----------

# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/Functions/Functions

# COMMAND ----------

playerDF = playerCheck(draft_Team=['Chicago Bulls'])
display(playeDF)

# COMMAND ----------


