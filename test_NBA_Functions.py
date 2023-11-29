# Databricks notebook source
# MAGIC %md
# MAGIC # Libary Imports

# COMMAND ----------

# MAGIC %sh 
# MAGIC pip install mock

# COMMAND ----------

import pyspark.sql.functions as F

from unittest.mock import Mock
import unittest
from unittest.mock import patch, MagicMock

from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split

from datetime import datetime
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, DoubleType, LongType

# COMMAND ----------

# MAGIC %run ./NBA_Functions

# COMMAND ----------

# DBTITLE 1,Test Data
#position = ["Center"]
#college = ["University of North Carolina"]
#draftTeam = ["Cleveland Cavaliers"]
#draftRound = [1]
#draftPick = []
#draftYear = []

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

        result_df = digitExtract()

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
                    StructField("salary", LongType(), True),
                    StructField("season_End", LongType(), True),
                    StructField("season_Start", LongType(), True)
                    ]
        
        table_schema = StructType(field_type)
        test_df = spark.createDataFrame([], table_schema)

        result_df = joinTable()

        test_df_types = test_df.dtypes
        result_df_types = result_df.dtypes

        self.assertEqual(test_df.schema, result_df.schema)
        self.assertEqual(test_df_types,result_df_types)

    @patch('__main__.position')
    @patch('__main__.college')
    @patch('__main__.draftTeam')
    @patch('__main__.draftRound')
    @patch('__main__.draftPick')
    @patch('__main__.draftYear')
    def test_Player_Check(self, mock_position, mock_college, mock_draft_Team, mock_draft_Round, mock_draft_Pick, mock_draft_Year):

        #playerCheck.mock_position.return_value.self_assert_called_with()
        result_df = playerCheck(mock_position, mock_college, mock_draft_Team, mock_draft_Round, mock_draft_Pick, mock_draft_Year)


# COMMAND ----------

# DBTITLE 1,Execute Test Class
suite = unittest.TestLoader().loadTestsFromTestCase(test_NBA_Functions)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)

# COMMAND ----------


