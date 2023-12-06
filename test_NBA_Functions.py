# Databricks notebook source
# MAGIC %md
# MAGIC # Libary Imports

# COMMAND ----------

import pyspark.sql.functions as F

import unittest

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split

from datetime import datetime
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, DoubleType, LongType

# COMMAND ----------

# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/NBA_Functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Unittests

# COMMAND ----------

# DBTITLE 1,Test Class
class test_NBA_Functions(unittest.TestCase): 

    def test_playerTable(self):

        result_df = playerTable()
        listColumns = result_df.columns

        self.assertTrue("player_Id" in listColumns)
        self.assertTrue("career_Ast" in listColumns)
        self.assertTrue("career_FG%" in listColumns)
        self.assertTrue("career_FG3%" in listColumns)
        self.assertTrue("career_FT%" in listColumns)
        self.assertTrue("career_G" in listColumns)
        self.assertTrue("career_PTS" in listColumns)
        self.assertTrue("career_TRB" in listColumns)
        self.assertTrue("career_FT%" in listColumns)
        self.assertTrue("college" in listColumns)
        self.assertTrue("draft_Pick" in listColumns)
        self.assertTrue("draft_Round" in listColumns)
        self.assertTrue("draft_Year" in listColumns)
        self.assertTrue("player_Name" in listColumns)
        self.assertTrue("position" in listColumns)

    def test_salaryTable(self):

        result_df = salaryTable()
        listColumns = result_df.columns

        self.assertTrue("player_Id" in listColumns)
        self.assertTrue("season_Start" in listColumns)
        self.assertTrue("season_End" in listColumns)
        self.assertTrue("salary" in listColumns)

    def test_positionCleaning(self):

        result_df = position_cleaning()
        expected_positions = ["Shooting_Guard", "Small Forward", "Guard", "Point Guard", "Power Forward", "Forward", "Center"]
    
        test_df = result_df.filter(F.col("position").isin(expected_positions))
        self.assertTrue(test_df)
    
    def test_digitDF(self): 

        #Before digitExtract Function
        test_df = position_cleaning()

        test_df1 = test_df.select("draft_Pick").dtypes
        test_df2 = test_df.select("draft_Round").dtypes

        self.assertTrue(test_df1[0][1] == "string")
        self.assertTrue(test_df2[0][1] == "string")

        #After digitExtract Function
        result_df = digitExtract()

        result_df1 = result_df.select("draft_Pick").dtypes
        result_df2 = result_df.select("draft_Round").dtypes

        self.assertTrue(result_df1[0][1] == "int")
        self.assertTrue(result_df2[0][1] == "int")

    def test_joinTable(self): 

        field_type = [StructField("player_Id", StringType(), True),
                    StructField("career_Ast", DoubleType(), True),
                    StructField("career_FG%", DoubleType(), True),
                    StructField("career_FG3%", DoubleType(), True),
                    StructField("career_FT%", DoubleType(), True),
                    StructField("career_G", IntegerType(), True),
                    StructField("career_PTS", DoubleType(), True),
                    StructField("career_TRB", DoubleType(), True),
                    StructField("college", StringType(), True),
                    StructField("draft_Pick", IntegerType(), True),
                    StructField("draft_Round", IntegerType(), True),
                    StructField("draft_Team", StringType(), True),
                    StructField("draft_Year", IntegerType(), True),
                    StructField("player_Name", StringType(), True),
                    StructField("position", StringType(), False), 
                    StructField("salary", StringType(), True),
                    StructField("season_End", StringType(), True),
                    StructField("season_Start", StringType(), True)
                    ]
        
        table_schema = StructType(field_type)
        test_df = spark.createDataFrame([], table_schema)

        result_df = joinTable()

        test_df_types = test_df.dtypes
        result_df_types = result_df.dtypes

        self.assertEqual(test_df.schema, result_df.schema)
        self.assertEqual(test_df_types,result_df_types)

    def test_Player_Check(self):

        #Position Parameter
        result_df1 = playerCheck(position=["Center", "Forward"])
        result_df2 = playerCheck(position=["Center"])
        self.assertNotEqual(result_df1.collect(),result_df2.collect())
        
        #College Parameter
        result_df3 = playerCheck(college=["University of Kentucky"])
        result_df4 = playerCheck(college=["Duke University"])
        self.assertNotEqual(result_df3.collect(),result_df4.collect())

        #DraftTeam Parameter
        result_df5 = playerCheck(draftTeam=["Chicago Bulls"])
        result_df6 = playerCheck(draftTeam=["Golden State Warriors"])
        self.assertNotEqual(result_df5.collect(),result_df6.collect())

        #DraftRound Parameter
        result_df7 = playerCheck(draftRound=[1])
        result_df8 = playerCheck(draftRound=[2])
        self.assertNotEqual(result_df7.collect(),result_df8.collect()) 

        #DraftPick Parameter
        result_df9 = playerCheck(draftPick=[1])
        result_df10 = playerCheck(draftPick=[13])
        self.assertNotEqual(result_df9.collect(),result_df10.collect()) 

        #DraftYear Parameter
        result_df11 = playerCheck(draftYear=[1998])
        result_df12 = playerCheck(draftYear=[2016])
        self.assertNotEqual(result_df11.collect(),result_df12.collect()) 

    def test_avg_player_salary(self): 

        test_df = playerCheck(draftTeam=["Chicago_Bulls"])
        result_df = avgPlayerSalary(test_df)
        result_columns = result_df.columns

        self.assertTrue("player_Id" in result_columns)
        self.assertTrue("count_Seasons" in result_columns)
        self.assertTrue("sum_Salary" in result_columns)
        self.assertTrue("avg_Salary" in result_columns)

    def test_team_salary_per_year(self):

        test_df = playerCheck(draftTeam=["Chicago_Bulls"])
        result_df = teamSalaryPerYear(test_df)    
        result_columns = result_df.columns

        self.assertTrue("draft_Team" in result_columns)
        self.assertTrue("draft_Year" in result_columns)
        self.assertTrue("total_Salary" in result_columns)

    def test_avg_salary_per_season(self): 

        test_df = playerCheck(draftTeam=["Chicago_Bulls"])
        result_df = avgSalaryBySeason(test_df)
        result_columns = result_df.columns

        self.assertTrue("season_Start" in result_columns)
        self.assertTrue("avg_Salary_YR" in result_columns)

    def test_avg_dollars_game(self): 

        test_df = playerCheck(draftTeam=["Chicago_Bulls"])
        result_df = avgDollarsGame(test_df)
        result_columns = result_df.columns

        self.assertTrue("player_Id" in result_columns)
        self.assertTrue("count_Seasons" in result_columns)
        self.assertTrue("sum_Salary" in result_columns)
        self.assertTrue("avg_Salary" in result_columns)
        self.assertTrue("avg_salary_per_g" in result_columns)

    def test_avg_dollars_point(self): 

        test_df = playerCheck(draftTeam=["Chicago_Bulls"])
        result_df = avgDollarsPoint(test_df)
        result_columns = result_df.columns

        self.assertTrue("draft_Team" in result_columns)
        self.assertTrue("avg_dollars_point" in result_columns)

# COMMAND ----------

# MAGIC %md
# MAGIC # Output

# COMMAND ----------

# DBTITLE 1,Execute Test Class
suite = unittest.TestLoader().loadTestsFromTestCase(test_NBA_Functions)
result = unittest.TextTestRunner().run(suite)

# COMMAND ----------

if result.wasSuccessful():
    print("All tests passed successfully!")
else: 
    raise Exception("One or more tests did not successfully pass. Please read log files to know more.")
