# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import concat, col, lit, current_timestamp, current_date, date_add, to_date, when, substring, month, sum, count, trim, unix_timestamp, from_unixtime, to_date, countDistinct, date_sub, array_join, concat_ws, collect_list, pow, first, row_number, date_format, split
from pyspark.sql.types import IntegerType