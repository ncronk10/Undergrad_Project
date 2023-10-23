# Databricks notebook source
# DBTITLE 1,Players Table
players = (spark.table('default.players')
           .withColumnRenamed('_id','player_Id')
           .withColumnRenamed('birthDate', 'birth_Date')
           .withColumnRenamed('birthPlace', 'birth_Place')
           .withColumnRenamed('career_AST', 'career_Ast')
           .withColumnRenamed('draft_pick', 'draft_Pick')
           .withColumnRenamed('draft_round', 'draft_Round')
           .withColumnRenamed('draft_team', 'draft_Team')
           .withColumnRenamed('draft_year', 'draft_Year')
           .drop('career_PER','career_TRB', 'career_WS', 'career_eFG%', 'shoots')
           .dropna())

# COMMAND ----------

# DBTITLE 1,Salaries Table
salaries = (spark.table('default.salaries')
            .withColumnRenamed('player_id', 'player_Id')
            .withColumnRenamed('season_end', 'season_End')
            .withColumnRenamed('season_start', 'season_Start')
            .dropna())
