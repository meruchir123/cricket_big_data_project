# Databricks notebook source
# DBTITLE 1,BronzeLayer
# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current

# COMMAND ----------

# DBTITLE 1,Importing Libraries
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Read the Bronze Table
bronze_df=spark.table("workspace.default.cricket_bronze_current")

raw_json=bronze_df.select("raw_json").collect()[0]['raw_json']
api_data=json.loads(raw_json)  #this is for converting raw api json data to python dictionary 

matches=api_data.get("data",[])  # This is a list

print("Total Matches found are: ",len(matches))
print(matches[0] if len(matches)>0 else "No matches found")

# COMMAND ----------

# DBTITLE 1,Extracting Useful fields
silver_rows=[]

for match in matches:
  teams=match.get("teams",[])
  score=match.get("score",[])
  team_1=teams[0] if len(teams)>0 else None
  team_2=teams[1] if len(teams)>1 else None

  score_1=None
  score_2=None


  # The error occurs because the code assumes every match has exactly 2 scores in the score list, but the actual data shows that some matches only have 1 score entry (because it's "Innings Break" - only the first innings is complete). The code tries to access score[1] when the list only has 1 element.

  if len(score)>0:
    score_1=f"{score[0].get('r',None)}/{score[0].get('w',None)} in {score[0].get('o',None)} overs"
  if len(score)>1:
    score_2=f"{score[1].get('r',None)}/{score[1].get('w',None)} in {score[1].get('o',None)} overs"

  silver_rows.append({
        "match_id": match.get("id"),
        "match_name": match.get("name"),
        "match_type": match.get("matchType"),
        "status": match.get("status"),
        "venue": match.get("venue"),
        "date": match.get("date"),
        "dateTimeGMT": match.get("dateTimeGMT"),
        "team_1": team_1,
        "team_2": team_2,
        "team_1_score": score_1,
        "team_2_score": score_2,
        "series_id": match.get("series_id"),
        "fantasyEnabled": match.get("fantasyEnabled"),
        "matchStarted": match.get("matchStarted"),
        "matchEnded": match.get("matchEnded")
})

print("Silver rows prepared:",len(silver_rows))

# COMMAND ----------

silver_schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("match_name", StringType(), True),
    StructField("match_type", StringType(), True),
    StructField("status", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("date", StringType(), True),
    StructField("dateTimeGMT", StringType(), True),
    StructField("team_1", StringType(), True),
    StructField("team_2", StringType(), True),
    StructField("team_1_score", StringType(), True),
    StructField("team_2_score", StringType(), True),
    StructField("series_id", StringType(), True),
    StructField("fantasyEnabled", BooleanType(), True),
    StructField("matchStarted", BooleanType(), True),
    StructField("matchEnded", BooleanType(), True)
])

silver_df = spark.createDataFrame(silver_rows, schema=silver_schema)\
    .withColumn("match_date",to_date(col("date")))\
    .withColumn("loaded_at",current_timestamp())
display(silver_df)

# COMMAND ----------

silver_df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .saveAsTable("workspace.default.cricket_silver_current")

print("Silver Table Created successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_silver_current
