# Databricks notebook source
# MAGIC %sh
# MAGIC databricks secrets -h

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks secrets list-scopes

# COMMAND ----------

# MAGIC %sh 
# MAGIC https://databricks/python3/bin/databricks configure

# COMMAND ----------

# MAGIC %sh 
# MAGIC databricks secrets list-scopes

# COMMAND ----------

# MAGIC %md
# MAGIC # Library Imports

# COMMAND ----------

import pyspark.sql.functions as F

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split

from datetime import datetime
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, DoubleType, LongType 

# COMMAND ----------

# MAGIC %md
# MAGIC # Player Table

# COMMAND ----------

# DBTITLE 1,Player Table 
def playerTable():
    """
    Author: Nathan Cronk 

    Description: This function reads in the players table that contains the data. Then the table is then cleaned with column renaming, dropping nulls and some unused columns, and specifies "double" datatypes for a few columns.
    
    Inputs: 
        - None

    Outputs:
        - playersDF: dataframe with columns renamed, casting double types to some columns, and dropping some columns and nulls  
    """

    playersDF = (spark.read.csv("s3a://myresearchproject/players.csv", header=True)
        .withColumnRenamed("_id","player_Id")
        .withColumnRenamed("name","player_Name")
        .withColumnRenamed("career_AST", "career_Ast")
        .withColumnRenamed("draft_pick", "draft_Pick")
        .withColumnRenamed("draft_round", "draft_Round")
        .withColumnRenamed("draft_team", "draft_Team")
        .withColumnRenamed("draft_year", "draft_Year")
        .withColumn("draft_Year", F.substring(F.col("draft_Year"), 0,4).cast(IntegerType()))
        .withColumn("career_G", F.col("career_G").cast(IntegerType()))
        .withColumn("career_PTS", F.col("career_PTS").cast(DoubleType()))
        .withColumn("career_Ast", F.col("career_Ast").cast(DoubleType()))
        .withColumn("career_TRB", F.col("career_TRB").cast(DoubleType()))
        .withColumn("career_FG%", when(F.col("career_FG%") == "-", 0.0).otherwise(F.col("career_FG%").cast(DoubleType())))
        .withColumn("career_FG3%", when(F.col("career_FG3%") == "-", 0.0).otherwise(F.col("career_FG3%").cast(DoubleType())))
        .withColumn("career_FT%", when(F.col("career_FT%") == "-", 0.0).otherwise(F.col("career_FT%").cast(DoubleType())))
        .drop("career_eFG%","highSchool", "shoots", "career_WS", "career_PER", "height", "weight", "birthDate", "birthPlace")
        .dropna()
    )
    
    return playersDF

# COMMAND ----------

# MAGIC %md
# MAGIC #Salary Table

# COMMAND ----------

# DBTITLE 1,Salary Table 
def salaryTable():
    """
    Author: Nathan Cronk 

    Description: This function reads in the salaries table that contains the data. Then the table is then cleaned with column renaming and dropping null values. 
    
    Inputs: 
        - None
    Outputs: 
        - salariesDF: dataframe with columns renamed, casting double types to some columns, and dropping some columns and nulls
    """

    salariesDF = (spark.read.csv("s3a://myresearchproject/salaries.csv", header=True)
            .withColumnRenamed("player_id", "player_Id")
            .withColumnRenamed("season_end", "season_End")
            .withColumnRenamed("season_start", "season_Start")
            .drop("team", "league", "season")
            .dropna()
    )

    return salariesDF

# COMMAND ----------

# MAGIC %md
# MAGIC # Regex Cleaning Functions

# COMMAND ----------

# DBTITLE 1,Position Cleaning
def position_cleaning():
    """
    Author: Nathan Cronk 

    Description: This function calls the playersDF dataframe. Then, the function cleans up the column position by extracting the first two words of the column, and doing some regex replacing. Regex is a column tool in data science used to replace, extract, and manipulate strings within a dataframe's column.
    
    Inputs: 
        - None
    Outputs: 
        - positionDF: dataframe with a "cleaned-up" position column 
    """

    playersDF = playerTable()
    
    split_text = split(F.col("position"), "\\s+")

    positionDF = playersDF.withColumn("extracted_words", concat_ws(" ", split_text[0], split_text[1]))
    
    positionDF = (positionDF
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Center/Forward", "Center"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Forward/Center", "Forward"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Forward/Guard", "Forward"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Guard/Forward", "Guard"))
            .withColumn("extracted_words", F.regexp_replace(F.col("extracted_words"), "Center and", "Center"))
            .drop("position")
            .withColumnRenamed("extracted_words", "position")
        )
    
    return positionDF
    

# COMMAND ----------

# DBTITLE 1,Digit Extract 
def digitExtract():
    """
    Author: Nathan Cronk 

    Description: This function calls the position cleaning function, and does more regex cleaning. In this function, some values in draft pick and draft round (ex: 1st round) were not useful, so this function uses regex extract to extract at most the first 
    three digits of the string, and then casting Integers to those associated columns.
    
    Inputs: 
        - None
    Outputs:
        - digitDF: dataframe with columns draft pick and draft round with their associated extracted digits, and newly casted integer datatypes. (could cast date to draft year, may implement later)
    """

    digitDF = position_cleaning()

    digitDF = (digitDF
        .withColumn("draft_Round", F.regexp_extract("draft_Round", "\d{1,3}(?!\d)", 0))
        .withColumn("draft_Pick", F.regexp_extract("draft_Pick", "\d{1,3}(?!\d)", 0))
        .withColumn("draft_Round", F.col("draft_Round").cast(IntegerType()))
        .withColumn("draft_Pick", F.col("draft_Pick").cast(IntegerType()))
    )

    return digitDF

# COMMAND ----------

# MAGIC %md
# MAGIC # Table Join function

# COMMAND ----------

# DBTITLE 1,Table Join 
def joinTable():
    """
    Author: Nathan Cronk 

    Description: This function calls the digit extract function and salary table function, and joins the two associated tables on the player_Id column 
    
    Inputs:
        - None

    Outputs:
        - joinDF: dataframe with joined tables (players and salries) and drops nulls as well. 
    """

    digitDF = digitExtract()
    salariesDF = salaryTable()

    joinDF = (digitDF.join(salariesDF, ["player_id"], how="left")
              .withColumnRenamed("_id","player_Id")
              .dropna()
              )
    
    return joinDF

# COMMAND ----------

# MAGIC %md
# MAGIC # NBA Player Check

# COMMAND ----------

# DBTITLE 1,NBA Player Check
def playerCheck(position: list = [], college: list = [], draftTeam: list = [], draftRound: list = [], draftPick: list = [], draftYear: list =[]):
    """
    Author: Nathan Cronk 

    Description: This function allows the user to filter the NBA Players table down to a specific position(s), college(s), draftTeam(s), draftRound(s), draftPick(s), or draftYear(s). All inputs are optional
    so if none are given, all players, teams, etc. will be in the resulting dataframe
    
    Inputs:
        - position (optional) - positions of players
        - colleges (optional) - college team where drafted players came from 
        - draftTeam (optional) - newly drafted NBA team 
        - draftRound (optional) - round of the draft in which the player was selected
        - draftPick (optional) - pick of the draft in which the player was selected
        - draft Year (optional) - year in which the player was selected

    Outputs: 
        - df: dataframe with associated filters from input
    """

    df = digitExtract()
    
    try:

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
            df = df.where(F.col("draft_Year").isin(draftYear))

    except:

        print("The input given to the function was not valid. Please try again.")

    return df 

# COMMAND ----------

# MAGIC %md
# MAGIC # Salary Computation Functions

# COMMAND ----------

# DBTITLE 1,Average Player Salary per Season 
def avgPlayerSalary(df): 
    """
    Author: Nathan Cronk 

    Description: This function takes in a dataframe from the playerCheck function, partitions by player_Id and counts the number of seasons and computes their average salary per season 
    
    Inputs:
        - df: dataframe from playerCheck function 

    Outputs: 
        - avgDF: dataframe with avg_Salary column, and sorted by avg_Salary in descending order
    """

    joinDF = joinTable()

    masterDF = joinDF.join(df, ["player_Id"], how="inner")

    window = Window.partitionBy("player_Id").orderBy(F.col('season_Start'))

    filterDF = (masterDF
                .groupBy("player_Id").agg(F.count("season_Start").alias("count_Seasons"),
                F.sum("salary").alias("sum_Salary")))     

    avgDF = filterDF.withColumn("avg_Salary", F.round(F.col("sum_Salary")/F.col("count_Seasons"), 2))  

    return avgDF.select("player_Id","count_Seasons","sum_Salary","avg_Salary").orderBy(F.col("avg_Salary").desc())

# COMMAND ----------

# DBTITLE 1,Team Salary per Year
def teamSalaryPerYear(df):
    """
    Author: Nathan Cronk 

    Description: This function takes in a dataframe from the playerCheck function. Then, the function groups by draft_Team and draft_Year and computes that team's total team salary per year. 
    
    Inputs: 
        - df: dataframe from playerCheck function

    Outputs: 
        - totalDF: dataframe with a total_Salary column, which indicates the team's total salary for that year 
    """

    joinDF = joinTable()
    
    masterDF= joinDF.join(df, ["player_Id", "draft_Team", "draft_Year"], how="inner")

    totalDF = masterDF.groupBy("draft_Team", "draft_Year").agg(F.sum("salary").alias("total_Salary"))

    return totalDF.orderBy(F.col("draft_Year"))


# COMMAND ----------

# DBTITLE 1,Average Salary per Season 
def avgSalaryBySeason(df): 
    """
    Author: Nathan Cronk 

    Description: This function takes in a dataframe, groups by the season year, and computes the average salary for that season. 

    Inputs: 
        - df: dataframe 

    Output:
        - result: dataframe with computed season average column, and sorted in ascending order by season start year
    """

    joinDF = joinTable()
    
    masterDF= joinDF.join(df, ["player_Id"], how="inner")

    result = (masterDF
              .groupBy("season_Start").agg(F.avg("salary").alias("avg_Salary_YR")))
    result = result.withColumn("avg_Salary_YR", F.round(F.col("avg_Salary_YR"), 2))

    return result.orderBy("season_Start")


# COMMAND ----------

# DBTITLE 1,Average Salary per Game 
def avgDollarsGame(df): 
    """
    Author: Nathan Cronk 

    Description: This function computes the average salary per game a player is paid. The function takes in a dataframe, and divides career games by the number of seasons they played. Then, that number is divided by their average salary. 

    Inputs: 
        - df: dataframe with associated columns in description 

    Outputs: 
        - resultDF: dataframe with new column avg_salary_per_g
    """
    
    joinDF = joinTable()

    masterDF = joinDF.join(df, ["player_Id", "career_G"], how="inner")

    filterDF = (masterDF.groupBy("player_Id", "career_G")
            .agg(F.count("season_Start").alias("season_Count")))

    result = filterDF.withColumn("avg_Games", F.round(F.col("career_G")/F.col("season_Count"), 2))  

    result = result.select("player_Id", "avg_Games").orderBy(F.col("avg_Games").desc()) 

    avgSalaryDF = avgPlayerSalary(df)

    resultDF = result.join(avgSalaryDF, ["player_Id"], how="left")
    resultDF = resultDF.withColumn("avg_salary_per_g", F.round(F.col("avg_Salary")/F.col("avg_Games"), 2))

    return resultDF

# COMMAND ----------

# DBTITLE 1,Average Salary per Point
def avgDollarsPoint(df): 
    """
    Author: Nathan Cronk 

    Description: This function takes in a dataframe, and computes average dollars per point that a player scores. To get this computation, we take their average salary per game and divide that by their career average points

    Inputs: 
        - df: data frame with avg_salary_per_g and career_PTS Columns

    Outputs: 
        - avgDollarsPointsDF: dataframe with column avg_dollars_point
    """
    
    avgDF = avgDollarsGame(df)
    #joinDF = joinTable()
    #joinDF = joinDF.join(df, ["player_Id"],how="left")

    masterDF = avgDF.join(df, ["player_Id"], how="inner")
    dollarsPointDF = masterDF.withColumn("dollars_point", F.round(F.col("avg_salary_per_g")/F.col("career_PTS"), 2))

    avgDollarsPointsDF = dollarsPointDF.groupBy("draft_Team").agg(F.avg(F.col("dollars_point")).alias("avg_dollars_point"))
    avgDollarsPointsDF = avgDollarsPointsDF.withColumn("avg_dollars_point", F.round(F.col("avg_dollars_point"), 2)).orderBy(F.col("avg_dollars_point").desc())

    return avgDollarsPointsDF
