# Databricks notebook source
import pyspark.sql.functions as F

from pyspark.sql.window import Window

from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split

from datetime import datetime
from pyspark.sql.types import IntegerType

# COMMAND ----------

def playerTable():

    playersDF = (spark.table("default.players")
        .withColumnRenamed("_id","player_Id")
        .withColumnRenamed("name","player_Name")
        .withColumnRenamed("birthDate", "birth_Date")
        .withColumnRenamed("birthPlace", "birth_Place")
        .withColumnRenamed("career_AST", "career_Ast")
        .withColumnRenamed("draft_pick", "draft_Pick")
        .withColumnRenamed("draft_round", "draft_Round")
        .withColumnRenamed("draft_team", "draft_Team")
        .withColumnRenamed("draft_year", "draft_Year")
        .withColumn("draft_Year", F.substring(F.col("draft_Year"), 0,4).cast("date"))
        .withColumn("draft_Year", F.year("draft_Year"))
        .withColumn("career_FG%", when(F.col("career_FG%") == "-", 0.0).otherwise(F.col("career_FG%").cast("double")))
        .withColumn("career_FG3%", when(F.col("career_FG3%") == "-", 0.0).otherwise(F.col("career_FG3%").cast("double")))
        .withColumn("career_FT%", when(F.col("career_FT%") == "-", 0.0).otherwise(F.col("career_FT%").cast("double")))
        .withColumn("career_PER", when(F.col("career_PER") == "-", 0.0).otherwise(F.col("career_PER").cast("double")))
        .drop("career_eFG%")
        .dropna()
    )
    
    return playersDF

# COMMAND ----------

def salaryTable():

    salariesDF = (spark.table("default.salaries")
            .withColumnRenamed("player_id", "player_Id")
            .withColumnRenamed("season_end", "season_End")
            .withColumnRenamed("season_start", "season_Start")
            .dropna()
    )

    return salariesDF

# COMMAND ----------

def joinTable():
    digitDF = digitExtract()
    salariesDF = salaryTable()

    joinDF = (digitDF.join(salariesDF, ["player_id"], how="left")
              .withColumnRenamed("_id","player_Id")
              .select("player_Id", "salary", "season_Start", "season_End")
              .dropna()
              )
    
    return joinDF

# COMMAND ----------

# DBTITLE 1,Position Cleaning
def position_cleaning():
    playersDF = playerTable()

    split_text = split(F.col("position"), "\\s+")

    fin = playersDF.withColumn("extracted_words", concat_ws(" ", split_text[0], split_text[1]))
    
    fin = (fin
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Center/Forward", "Center"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Forward/Center", "Forward"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Forward/Guard", "Forward"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Guard/Forward", "Guard"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Center and", "Center"))
            .drop("position")
            .withColumnRenamed("extracted_words", "position")
        )
    
    return fin
    

# COMMAND ----------

def digitExtract():
    
    position = position_cleaning()

    fin = (position
        .withColumn("draft_Round", F.regexp_extract("draft_Round", "\d{1,3}(?!\d)", 0))
        .withColumn("draft_Pick", F.regexp_extract("draft_Pick", "\d{1,3}(?!\d)", 0))
        .withColumn("draft_Round", F.col("draft_Round").cast(IntegerType()))
        .withColumn("draft_Pick", F.col("draft_Pick").cast(IntegerType()))
    )

    return fin

# COMMAND ----------

# DBTITLE 1,PlayerCheck Function
def playerCheck(playerID: list = [], position: list = [], college: list = [], draftTeam: list = [], draftRound: list = [], draftPick: list = [], draftYear: list =[]):
    df = digitExtract()
    
    try:
        if playerID:
            df = df.where(F.col("player_Id").isin(playerID))

        if position:
            df = df.where(F.col("position").isin(position))
    
        if college:
            df = df.where(F.col("college").isin(college))
        
        if draftTeam:
            df = df.where(F.col("draft_Team").isin(draftTeam))

        if draftRound:
            df = df.where(F.col("draft_Round").isin(draftRound))

        if draftPick:
            df = df.where(F.col("draft_Pick").isin(draftPick))

        if draftYear:
            df = df.where(F.col('draft_Year').isin(draftYear))

    except:

        print("The input given to the function was not valid. Please try again.")

    return df 

# COMMAND ----------

# DBTITLE 1,topSalary Function
def topSalary(df, contract_Length:int=4):

    joinDF = joinTable()
    joinDF = joinDF.withColumn("yearly_Average", F.col("salary")/contract_Length)
    
    masterDF = df.join(joinDF, ["player_Id"], how='inner')

    window = Window.partitionBy("player_Id").orderBy(F.col("salary").desc())
    filterDF = masterDF.withColumn("temp", row_number().over(window)).filter(F.col("temp") == 1).drop("temp")

    return filterDF.orderBy(F.col("salary").desc()).orderBy(F.col("yearly_Average").desc())

# COMMAND ----------

# DBTITLE 1,teamSalaryPerYear
def teamSalaryPerYear(df):

    joinDF = joinTable()
    
    masterDF= joinDF.join(df, ["player_Id"], how="inner")

    window = Window.partitionBy("player_Id").orderBy(F.col("salary").desc())
    filterDF = masterDF.withColumn("temp", row_number().over(window)).filter(F.col("temp") == 1).drop("temp")

    result = filterDF.groupBy("draft_Team", "draft_Year").agg(F.sum("salary").alias("total_Salary"))

    return result.orderBy("draft_Year")


# COMMAND ----------

# DBTITLE 1,Avg Salary by Season 
def avgSalaryBySeason(df): 

    joinDF = joinTable()
    
    masterDF= joinDF.join(df, ["player_Id"], how="inner")

    window = Window.partitionBy("player_Id").orderBy(F.col("salary").desc())
    filterDF = masterDF.withColumn("temp", row_number().over(window)).filter(F.col("temp") == 1).drop("temp")

    result = (filterDF
              .groupBy("season_Start").agg(F.avg("salary").alias("avg_Salary_YR")))

    return result.orderBy("season_Start")

