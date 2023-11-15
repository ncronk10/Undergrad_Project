# Databricks notebook source
import pyspark.sql.functions as F

from pyspark.sql.window import Window

from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split

from datetime import datetime
from pyspark.sql.types import IntegerType

# COMMAND ----------

# DBTITLE 1,Player Table Function
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
        .drop("career_eFG%","highSchool")
        .dropna()
    )
    
    return playersDF

# COMMAND ----------

# DBTITLE 1,Salary Table Function
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

# DBTITLE 1,digitExtract Function
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

# DBTITLE 1,Avg Salary per Player (by total salary / total seasons played) 
def avgPlayerSalary(df): 
    
    joinDF = joinTable()

    masterDF = joinDF.join(df, ["player_Id"], how="inner")

    window = Window.partitionBy("player_Id").orderBy(F.col('season_Start'))

    filterDF = (masterDF.groupBy("player_Id").agg(
    F.count("season_Start").alias("count_Seasons"),
    F.sum("salary").alias("sum_Salary")))     

    avgDF = filterDF.withColumn("avg_Salary", F.round(F.col("sum_Salary")/F.col("count_Seasons"), 2))  

    return avgDF.select("player_Id","count_Seasons","avg_Salary").orderBy(F.col("avg_Salary").desc())

# COMMAND ----------

# DBTITLE 1,teamSalaryPerYear
def teamSalaryPerYear(df):

    joinDF = joinTable()
    
    masterDF= joinDF.join(df, ["player_Id"], how="inner")

    result = masterDF.groupBy("draft_Team", "draft_Year").agg(F.sum("salary").alias("total_Salary"))

    return result.orderBy(F.col("draft_Year"))


# COMMAND ----------

# DBTITLE 1,Avg Salary per Season by Player
def avgSalaryBySeason(df): 

    joinDF = joinTable()
    
    masterDF= joinDF.join(df, ["player_Id"], how="inner")

    result = (masterDF
              .groupBy("season_Start").agg(F.avg("salary").alias("avg_Salary_YR")))
    result = result.withColumn("avg_Salary_YR", F.round(F.col("avg_Salary_YR"), 2))

    return result.orderBy("season_Start")


# COMMAND ----------

# DBTITLE 1,Avg Money Paid per game 
def avgDollarsGame(df): 
    joinDF = joinTable()

    masterDF = joinDF.join(df, ["player_Id"], how="inner")

    filterDF = (masterDF.groupBy("player_Id", "career_G")
            .agg(F.count("season_Start").alias("season_Count")))

    result = filterDF.withColumn("avg_Games", F.round(F.col("career_G")/F.col("season_Count"), 2))  

    result = result.select("player_Id", "avg_Games").orderBy(F.col("avg_Games").desc()) 

    avgSalaryDF = avgPlayerSalary(df)

    resultDF = result.join(avgSalaryDF, ["player_Id"], how="left")
    resultDF = resultDF.withColumn("avg_salary_per_g", F.round(F.col("avg_Salary")/F.col("avg_Games"), 2))

    return resultDF

# COMMAND ----------

# DBTITLE 1,Avg Dollars Per Point
def avgDollarsPoint(df): 

    avgDF = avgDollarsGame(df)
    joinDF = joinTable()
    joinDF = joinDF.join(df, ["player_Id"],how="left")

    masterDF = avgDF.join(joinDF, ["player_Id"], how="left")
    dollarsPointDF = masterDF.withColumn("dollars_point", F.round(F.col("avg_salary_per_g")/F.col("career_PTS"), 2))

    avgDollarsPointsDF = dollarsPointDF.groupBy("draft_Year").agg(F.avg(F.col("dollars_point")).alias("avg_dollars_point"))
    avgDollarsPointsDF = avgDollarsPointsDF.withColumn("avg_dollars_point", F.round(F.col("avg_dollars_point"), 2)).orderBy(F.col("draft_Year").asc())

    return avgDollarsPointsDF
