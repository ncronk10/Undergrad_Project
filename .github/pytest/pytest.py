# Databricks notebook source
# MAGIC %md
# MAGIC # Libary Imports

# COMMAND ----------

# MAGIC %run /Repos/ncronk10@gmail.com/Undergrad_Project/Functions/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Window Creator Function

# COMMAND ----------

def spark_session(request):
    #Adopted this code to create a spark window to allow for testing (like a test environment)
    spark = (SparkSession.builder 
        .appName("pytest-pyspark") 
        .master("local[2]") 
        .getOrCreate()
    )
    
    request.addfinalizer(lambda: spark.stop())

    return spark

# COMMAND ----------

def setup_method():

    spark_session.sql("CREATE TABLE default.players AS SELECT * FROM VALUES ('1', 'Nate Cronk', '2002-01-08', 'Urbandale', '10', '1', 'Team', '2022', '40%', '30%', '80%', '15.5', 'highschool', 'Center') AS players(player_Id, player_Name, birth_Date, birth_Place, career_Ast, draft_Pick, draft_Team, draft_Year, career_FG%, career_FG3%, career_FT%, career_PER, career_eFG%, highSchool, position)")


# COMMAND ----------

# MAGIC %md
# MAGIC # Player Table pytests

# COMMAND ----------

# Fixture to create a Spark session for testing
@pytest.fixture(scope="session")
def test_playerTable(spark_session):

    test_data = [("cronkn01", "Nate Cronk", "2002-01-08", "Urbandale", "10", "1", "J-Hawks", "2022", "0.4", "0.3", "0.8", "15.5", "high", "randomHighShchool", "Guard"),
                ("smithj01", "John Smith", "1985-02-02", "Johnston", "12", "2", "Dragons", "2018", "-", "0.4", "0.75", "18.0", "low", "SomeHighSchool", "Forward")]

    original_columns = ["_id", "name", "birthDate", "birthPlace", "career_AST", "draft_pick", "draft_team", "draft_year",
               "career_FG%", "career_FG3%", "career_FT%", "career_PER", "career_eFG%", "highSchool", "postion"]
    
    test_df = spark_session.createDataFrame(test_data, original_columns)

    result_df = playerTable(test_df)

    expected_data = [("cronkn01", "Nate Cronk", "2002-01-08", "Urbandale", "10", "1", "J-Hawks", 2022, 0.4, 0.3, 0.8, 15.5, "Guard"),
                     ("smithj01", "John Smith", "1985-02-02", "Johnston", "12", "2", "Dragons", 2018, 0.0, 0.4, 0.75, 18.0, "Forward")]
    
    expected_columns = ["player_Id", "player_Name", "birth_Date", "birth_Place", "career_Ast", "draft_Pick", "draft_Team",
                        "draft_Year", "career_FG%", "career_FG3%", "career_FT%", "career_PER", "position"]

    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    #Column assertion testing
    assert "player_Id" in result_df.columns()
    assert "player_Name" in result_df.columns()
    assert "birth_Date" in result_df.columns()
    assert "birth_Place" in result_df.columns()
    assert "career_Ast" in result_df.columns()
    assert "draft_Pick" in result_df.columns()
    assert "draft_Team" in result_df.columns()
    assert "draft_Year" in result_df.columns()
    assert "career_FG%" in result_df.columns()
    assert "career_FG3%" in result_df.columns()
    assert "career_FT%" in result_df.columns()
    assert "career_PER" in result_df.columns()
    assert "position" in result_df.columns()

    #Confirming mock data is in mock dataframe
    assert result_df.collect() == expected_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Salary Table pytests

# COMMAND ----------

# Fixture to create a Spark session for testing
@pytest.fixture(scope="session")
def test_salaryTable(spark_session):

    test_data = [("cronkn01", "2022-01-01", "2022-12-31", 1000000),
                ("smithj01", "2021-01-01", "2021-12-31", 800000),
                ("hendersonm01", "2023-01-01", "2023-12-31", None)]

    original_columns = ["player_id", "season_end", "season_start", "salary"]
    test_df = spark_session.createDataFrame(test_data, original_columns)

    result_df = salaryTable(test_df)

    expected_data = [("cronkn01", "2022-01-01", "2022-12-31", 1000000),
                     ("smithj01", "2021-01-01", "2021-12-31", 800000)]
    expected_columns = ["player_Id", "season_End", "season_Start", "salary"]

    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    #Column assertion testing
    assert "player_Id" in result_df.columns()
    assert "season_End" in result_df.columns()
    assert "season_Start" in result_df.columns()
    assert "salary" in result_df.columns()
    
    #Confirming mock data is in mock dataframe
    assert result_df.collect() == expected_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Regex Cleaning pytests

# COMMAND ----------

# Fixture to create a Spark session for testing
@pytest.fixture(scope="session")
def test_positionCleaning(spark_session):

    test_data = [("cronkn01", "Guard/Forward"),
                ("smithj01", "Guard"),
                ("hendersonm01", "Forward/Center"),
                ("deerej01", "Center and")]
    test_columns = ["player_Id", "position"]

    test_df = spark_session.createDataFrame(test_data, test_columns)
    
    #spark_session.sql("CREATE TABLE default.players AS SELECT * FROM VALUES ('1', 'Nate Cronk', '2002-01-08', 'Urbandale', '10', '1', 'Team', '2022', '40%', '30%', '80%', '15.5', 'highschool', 'Center') AS players(player_Id, player_Name, birth_Date, birth_Place, career_Ast, draft_Pick, draft_Team, draft_Year, career_FG%, career_FG3%, career_FT%, career_PER, career_eFG%, highSchool, position)")

    result_df = position_cleaning()

    expected_data = [("cronkn01", "Guard"),
                     ("smithj01", "Guard"),
                     ("hendersonm01", "Forward"),
                     ("deerej01", "Center")]

    expected_columns = ["player_Id", "position"]
    expected_positions = ["Shooting_Guard", "Small Forward", "Guard", "Point Guard", "Power Forward", "Forward", "Center"]
    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    assert result_df.collect() == expected_df.collect()
    assert result_df.position in expected_positions


# COMMAND ----------

# Fixture to create a Spark session for testing
@pytest.fixture(scope="session")
def test_digitDF(spark_session):

    test_data = [("cronkn01","Center", "1st round", "5th pick"),
                ("smithj01","Forward", "2nd round", "10th pick"),
                ("deerej01", "Guard", "3rd round", "15th pick")]
    test_columns = ["player_Id", "position", "draft_Round", "draft_Pick"]

    test_df = spark_session.createDataFrame(test_data, test_columns)

    #spark_session.sql("CREATE TABLE default.players AS SELECT * FROM VALUES ('1', 'Nate Cronk', '2002-01-08', 'Urbandale', '10', '1', 'Team', '2022', '40%', '30%', '80%', '15.5', 'highschool', 'Center') AS players(player_Id, player_Name, birth_Date, birth_Place, career_Ast, draft_Pick, draft_Team, draft_Year, career_FG%, career_FG3%, career_FT%, career_PER, career_eFG%, highSchool, position)")

    df = position_cleaning()
    result_df = digit_extract()

    expected_data = [("cronkn01","Center", 1, 5),
                    ("smithj01","Forward", 2, 10),
                    ("deerej01", "Guard", 3, 15)]
    
    expected_columns = ["player_Id", "position", "draft_Round", "draft_Pick"]
    
    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    assert result_df.collect() == expected_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC # Table Join pytests

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Player Check pytests

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Salary Functions pytests

# COMMAND ----------


