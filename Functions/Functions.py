# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format
from pyspark.sql.types import ArrayType, FloatType, StringType, IntegerType

# COMMAND ----------

# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/Tables/Tables

# COMMAND ----------

def playerCheck(df, playerID: list = [], playerName: list = [], position: list = [], height: list = [], weight: list = [], college: list = [], draftTeam: list = [], draftRound: list = [], draftPick: list = [], draftYear: list =[]):

    df = (spark.read.table('default.players').select('_id','name','position','height','weight','college','draft_team','draft_round','draft_pick','draft_year').withColumnRenamed('_id','player_id'))
    
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

df = players
#display(df)

# COMMAND ----------


