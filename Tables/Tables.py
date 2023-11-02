# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split
from pyspark.sql.types import IntegerType

# COMMAND ----------

# DBTITLE 1,Players Table
players = (spark.table('default.players')
           .withColumnRenamed('_id','player_Id')
           .withColumnRenamed('name','player_Name')
           .withColumnRenamed('birthDate', 'birth_Date')
           .withColumnRenamed('birthPlace', 'birth_Place')
           .withColumnRenamed('career_AST', 'career_Ast')
           .withColumnRenamed('draft_pick', 'draft_Pick')
           .withColumnRenamed('draft_round', 'draft_Round')
           .withColumnRenamed('draft_team', 'draft_Team')
           .withColumnRenamed('draft_year', 'draft_Year')
           .withColumn('draft_Year', F.col('draft_Year').cast(IntegerType()))
           .drop('career_PER','career_TRB', 'career_WS', 'career_eFG%', 'shoots')
           .dropna())

# COMMAND ----------

# DBTITLE 1,Salaries Table
salaries = (spark.table('default.salaries')
            .withColumnRenamed('player_id', 'player_Id')
            .withColumnRenamed('season_end', 'season_End')
            .withColumnRenamed('season_start', 'season_Start')
            .dropna())

# COMMAND ----------


