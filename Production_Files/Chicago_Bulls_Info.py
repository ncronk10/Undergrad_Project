# Databricks notebook source
# MAGIC %md
# MAGIC ### Chicago Bulls Historical Data
# MAGIC

# COMMAND ----------

# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/NBA_Functions

# COMMAND ----------

# DBTITLE 1,Pulling Chicago Bulls Data
playerDF = playerCheck(draftTeam=["Chicago Bulls"])
display(playerDF)

# COMMAND ----------

# DBTITLE 1,Area Chart for Team Salary per Year
teamSalary = teamSalaryPerYear(playerDF)
display(teamSalary)

# COMMAND ----------

# DBTITLE 1,Area Chart for Average Salary per Year 
avgSeasonSalary = avgSalaryBySeason(playerDF)
display(avgSeasonSalary)

# COMMAND ----------

# DBTITLE 1,Data Profile with Statistics for Average Dollars a player makes per game
avgGameDF = avgDollarsGame(playerDF)
display(avgGameDF)

# COMMAND ----------

# DBTITLE 1,Area Chart for Average Dollars Made Per Point Scored
df = playerCheck()

avgPointDF = avgDollarsPoint(df) #Change draft year to date (year)
display(avgPointDF)
