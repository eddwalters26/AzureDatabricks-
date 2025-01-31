# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DateType,
    TimestampType,
    ArrayType,
)

# COMMAND ----------

TARGET_ROWS = 20_000_000
FY_COLUMN_NAME = "Der_Financial_Year"
CONTROL_FILE_DATUM = "Appointment_Date"
BASE_SOURCE_PATH = "abfss://"
BASE_EXTRACT_PATH = "abfss://"

# COMMAND ----------

entities = {
    "OPA": {
        "basePath": BASE_SOURCE_PATH + "/OPA_Core/Published/1",
        "children": {
            "OPADiagnosis": {
                "basePath": BASE_SOURCE_PATH + "/OPA_Diag/Published/1"
            },
            "OPAProcedure": {
                "basePath": BASE_SOURCE_PATH + "/OPA_Proc/Published/1"
            }
        }
    }
}

# COMMAND ----------

# Define any schemas used in this Notebook
controlFileEntrySchema = StructType(
    [
        StructField("cExtractFileName", ArrayType((StringType()))),
        StructField("nRecordCount", IntegerType()),
        StructField("cDatum", StringType()),
        StructField("dPeriodStart", TimestampType()),
        StructField("dPeriodEnd", TimestampType()),
    ]
)

currentHashSchema = StructType(
    [
        StructField("OPA_Ident", LongType()),
        StructField(FY_COLUMN_NAME, StringType()),
        StructField("Hash", StringType()),
        StructField("hash_count", LongType()),
    ]
)

fy_folders = StructType(
    [
        StructField("DataSource", StringType()),
        StructField("Folder", StringType()),
        StructField("LastModified", TimestampType()),
    ]
)

# COMMAND ----------

import json

config_path = BASE_EXTRACT_PATH + "/Config/OPAPlusConfig.json"

j = dbutils.fs.head(config_path, max_bytes=1000000)
config_data = json.loads(j)["entities"]

# COMMAND ----------

# MAGIC %run "./Extract Data Pipeline"

# COMMAND ----------

# Read the state of the folders from when the last run was performed
try:
    fy_path = BASE_EXTRACT_PATH + "/OPAPlus_PreviousFolders"
    prev_FYFolders = spark.read.schema(fy_folders).json(fy_path)
except Exception as e:
    prev_FYFolders = spark.createDataFrame([], fy_folders)

prev_FYFolders = prev_FYFolders.coalesce(1)

# COMMAND ----------

currFYState = {"curr_FYFolders": spark.createDataFrame([], fy_folders)}
process_entities(entities, [process_extract_folders], [process_extract_folders], currFYState)

curr_FYFolders = currFYState["curr_FYFolders"].coalesce(1)
currFYState = {}

# Transform folder name into a derived financial year FY2012-13 -> 2012/13
extract_FinanicalYears = (
    curr_FYFolders.join(prev_FYFolders, on=["DataSource", "Folder", "LastModified"], how="left_anti")
    .select("Folder"
            , regexp_replace("Folder", "^FY(\d{4})-(\d{2})/$", "$1/$2").alias(FY_COLUMN_NAME)
            , regexp_replace("Folder", "^FY(\d{4})-(\d{2})/$", "$1").alias("FYFilter"))
    .filter("FYFilter >= 2015")
    .distinct()
    .sort("Folder")
)

extract_FYFolders = sorted(extract_FinanicalYears.agg(collect_set("Folder").alias("Folder")).collect()[0]["Folder"])

if extract_FinanicalYears.count() == 0:
    dbutils.notebook.exit("No changes to FY folders found")
    exit(0)

entitiesState = {"entities": entities.copy()}

process_entities(entities, [set_extract_paths], [set_extract_paths], entitiesState)

entities = entitiesState["entities"]
entitiesState = {}

# COMMAND ----------

state = {}
state["providers"] = getProvidersOrgs()
state["gp"] = getGPOrgs()

process_entities(entities, [process_parent_keys], None, state)

for df in state.values():
    df.unpersist()

state.clear()

# COMMAND ----------

keysState = {}

process_entities(entities, [process_current_hash], [process_current_hash], keysState)

for df in keysState.values():
    df.unpersist()

keysState.clear()

# COMMAND ----------

extractKeysState = {}

process_entities(entities, [process_hash_changes], [process_hash_changes], extractKeysState)
process_entities(entities, [reduce_keys], None, extractKeysState)

extractKeysState.clear()

# COMMAND ----------

controlFileEntry = {}
extractKeysState = {}

process_entities(entities, [extract_data_changes], [extract_data_changes], extractKeysState)

for df in extractKeysState.values():
    df.unpersist()
extractKeysState.clear()

# COMMAND ----------

process_entities(entities, [extract_csv], [extract_csv], None)

# COMMAND ----------

process_entities(entities, [move_extract_csv], [move_extract_csv], None)

# COMMAND ----------

process_entities(entities, [storeCurrentParentKeys], [storeCurrentChildKeys], None) 
         

# COMMAND ----------

process_entities(entities, [entity_cleanup], [entity_cleanup], None) 


# COMMAND ----------

writeControlFile("OPAPlus")


# COMMAND ----------

fy_path = BASE_EXTRACT_PATH + "/OPAPlus_PreviousFolders"
curr_FYFolders.write.mode("overwrite").json(fy_path)