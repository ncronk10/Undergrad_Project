# Databricks notebook source
# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/Tables/Tables

# COMMAND ----------

# DBTITLE 1,PlayerCheck Function
def playerCheck(playerID: list = [], position: list = [], college: list = [], draftTeam: list = [], draftRound: list = [], draftPick: list = [], draftYear: list =[]):
    
    df = (players
          .select('player_Id','player_Name','position','height','weight','college','draft_Team','draft_Round','draft_Pick','draft_Year')
          .withColumn('draft_Round', F.regexp_extract('draft_Round', '\d{1,3}(?!\d)', 0))
          .withColumn('draft_Pick', F.regexp_extract('draft_Pick', '\d{1,3}(?!\d)', 0))
          .withColumn('draft_Round', F.col('draft_Round').cast(IntegerType()))
          .withColumn('draft_Pick', F.col('draft_Pick').cast(IntegerType()))
    )
    
    try:
        if playerID:
            df = df.where(F.col('player_Id').isin(playerID))

        if position:
            df = df.where(F.col('position').isin(position))
    
        if college:
            df = df.where(F.col('college').isin(college))
        
        if draftTeam:
            df = df.where(F.col('draft_Team').isin(draftTeam))

        if draftRound:
            df = df.where(F.col('draft_Round').isin(draftRound))

        if draftPick:
            df = df.where(F.col('draft_Pick').isin(draftPick))

        if draftYear:
            df = df.where(F.col('draft_Year').isin(draftYear))

        split_text = split(F.col("position"), "\\s+")
        df = df.withColumn("extracted_words", concat_ws(" ", split_text[0], split_text[1]))
        df = (df
                .withColumn("position", F.regexp_replace(F.col("extracted_words"), "Forward/Center", "Center"))
                .withColumn("position", F.regexp_replace(F.col("extracted_words"), "Center and", "Center"))
                .drop('extracted_words')
        )
        
        
    except:

        print("The input given to the function was not valid. Please try again.")

    return df 

# COMMAND ----------

# DBTITLE 1,topSalary Function
def topSalary(df, draftRound: list=[], draftPick: list=[], contract_Length:int=4):

    playersDF = players
    salariesDF = salaries

    fin = playerCheck()

    joinDF = (playersDF.join(salariesDF, ['player_id'], how='left')
              .withColumnRenamed('_id','player_Id')
              .select('player_Id', 'salary')
              .dropna()
              .withColumn('yearly_Average',F.col('salary')/contract_Length)
              )
    
    masterDF= df.join(joinDF, ['player_Id'], how='inner')
    
    try: 
        if draftRound:
            masterDF = masterDF.where(F.col('draft_Round').isin(draftRound))

        if draftPick:
            masterDF = masterDF.where(F.col('draft_Pick').isin(draftPick))  
    except: 
        
        print("The input given to the function was not valid. Please try again.")
    
    window = Window.partitionBy('player_Id').orderBy(F.col('salary').desc())
    filterDF = masterDF.withColumn('temp', row_number().over(window)).filter(F.col('temp') == 1).drop('temp')

    return filterDF.orderBy(F.col('salary').desc()).orderBy(F.col('yearly_Average').desc())

# COMMAND ----------

# DBTITLE 1,TeamSalaryPerYear
def TeamSalaryPerYear(df): 

    playersDF = players
    salariesDF = salaries

    fin = playerCheck()

    joinDF = (playersDF.join(salariesDF, ['player_id'], how='left')
              .withColumnRenamed('_id','player_Id')
              .select('player_Id', 'salary')
              .dropna()
              )
    
    masterDF= df.join(joinDF, ['player_Id'], how='inner') 

    window = Window.partitionBy('player_Id').orderBy(F.col('salary').desc())
    filterDF = masterDF.withColumn('temp', row_number().over(window)).filter(F.col('temp') == 1).drop('temp')

    result = filterDF.groupBy('draft_Team', 'draft_Year').agg(F.sum('salary').alias('total_Salary'))

    return result.orderBy(F.col('draft_Year')).orderBy(F.col('total_Salary'))


# COMMAND ----------


