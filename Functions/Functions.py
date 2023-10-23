# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split
from pyspark.sql.types import ArrayType, FloatType, StringType, IntegerType

# COMMAND ----------

# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/Tables/Tables

# COMMAND ----------

# DBTITLE 1,PlayerCheck 
def playerCheck(playerID: list = [], playerName: list = [], position: list = [], height: list = [], weight: list = [], college: list = [], draftTeam: list = [], draftRound: list = [], draftPick: list = [], draftYear: list =[]):

    df = (players
          .withColumnRenamed('_id','player_id')
          .select('player_id','name','position','height','weight','college','draft_team','draft_round','draft_pick','draft_year')
          .dropna())
    
    try:
        if playerID:
            df = df.where(F.col('player_id').isin(playerID))

        if playerName:
            df = df.where(F.col('name').isin(playerName))

        if position:
            df = df.where(F.col('position').isin(position))
        
        if height:
            df = df.where(F.col('height').isin(height))

        if weight:
            df = df.where(F.col('weight').isin(weight))
    
        if college:
            df = df.where(F.col('college').isin(college))
        
        if draftTeam:
            df = df.where(F.col('draft_team').isin(draftTeam))

        if draftRound:
            df = df.where(F.col('draft_round').isin(draftRound))

        if draftPick:
            df = df.where(F.col('draft_pick').isin(draftPick))

        if draftYear:
            df = df.where(F.col('draft_year').isin(draftYear))
        
    except:

        print("The input given to the function was not valid. Please try again.")

    return df 

# COMMAND ----------

# DBTITLE 1,topSalary
#Create a function that takes in a dataframe, round, pick, top x% of players based on salary and returns them
def topSalary(draftRound: list=[], draftPick: list=[]):

    playersDF = players.dropna()
    salariesDF = salaries.dropna()

    masterDF = (playersDF.join(salariesDF, ['player_id'], how='left')
              .withColumnRenamed('_id','player_id')
              .dropna()
              .select('player_id', 'name', 'height', 'weight', 'college', 'draft_team', 'draft_round', 'draft_pick', 'draft_year', 'salary')
              .withColumn('yearly_average',F.col('salary')/4)
              )

    #df = playerCheck(df)
    try: 
        if draftRound:
            df = masterDF.where(F.col('draft_round').isin(draftRound))

        if draftPick:
            df = masterDF.where(F.col('draft_pick').isin(draftPick))  
    except: 
        
        print("The input given to the function was not valid. Please try again.")

    return df.orderBy(F.col('salary').desc())

# COMMAND ----------

df = playerCheck(draftRound=['1st round'], college=["Duke University"])
#display(df)

# COMMAND ----------

d2 = topSalary(draftRound=["1st round"], draftPick=["2nd overall"])
display(d2)

# COMMAND ----------


