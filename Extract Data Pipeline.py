# Databricks notebook source
# Common imports
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
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
from pyspark.sql.column import Column
from pyspark.sql.functions import (
    sha2,
    col,
    concat,
    concat_ws,
    lit,
    upper,
    expr,
    to_date,
    collect_set,
    broadcast,
    regexp_replace,
    explode,
)
from datetime import datetime
from pathlib import Path
from functools import reduce

# Get salt secret
SALT = dbutils.secrets.get(scope="<scope>", key="<key>")

# COMMAND ----------

if "TARGET_ROWS" not in locals():
    TARGET_ROWS = 10_000_000

if "FY_COLUMN_NAME" not in locals():
    FY_COLUMN_NAME = "Dummy Column"

if "CONTROL_FILE_DATUM" not in locals():
    CONTROL_FILE_DATUM = "Dummy Date"

if "BASE_SOURCE_PATH" not in locals():
    BASE_SOURCE_PATH = "Dummy Path"

if "BASE_EXTRACT_PATH" not in locals():
    BASE_EXTRACT_PATH = "Dummy Path"

if "entities" not in locals():
    entities = {}

if "config_data" not in locals():
    config_data = {}

if "extract_FYFolders" not in locals():
    extract_FYFolders = []

if "extract_FinanicalYears" not in locals():
    extract_FinanicalYears = spark.createDataFrame([], StructType([StructField("col", StringType())]))

if "controlFileEntry" not in locals():
    controlFileEntry = {}

# Define any schemas used in this Notebook
if "controlFileEntrySchema" not in locals():
    controlFileEntrySchema = StructType(
        [
            StructField("cExtractFileName", ArrayType((StringType()))),
            StructField("nRecordCount", IntegerType()),
            StructField("cDatum", StringType()),
            StructField("dPeriodStart", TimestampType()),
            StructField("dPeriodEnd", TimestampType()),
        ]
    )

if "currentHashSchema" not in locals():
    currentHashSchema = StructType(
        [
            StructField("APCS_Ident", LongType()),
            StructField(FY_COLUMN_NAME, StringType()),
            StructField("Hash", StringType()),
            StructField("hash_count", LongType()),
        ]
    )

if "fy_folders" not in locals():
    fy_folders = StructType(
        [
            StructField("DataSource", StringType()),
            StructField("Folder", StringType()),
            StructField("LastModified", TimestampType()),
        ]
    )

# COMMAND ----------


def get_latest_modification_time(data_source: str, directory_path: str) -> list:
    try:
        fy_mod_dates = []
        # List all items in the specified directory
        base_folders = dbutils.fs.ls(directory_path)

        for folder in base_folders:
            if folder.isDir:
                # List all items in the subdirectory
                files = dbutils.fs.ls(folder.path)
                # Get modification times of all Parquet files
                modification_times = [
                    f.modificationTime for f in files if f.name.endswith(".parquet")
                ]

                if modification_times:
                    # Find the latest modification time
                    max_modification_time = max(modification_times)
                    max_modification_datetime = datetime.fromtimestamp(
                        max_modification_time / 1000
                    )

                    # Append the data source, folder name, and last modified time to the list
                    fy_mod_dates.append(
                        {
                            "DataSource": data_source,
                            "Folder": folder.name,
                            "LastModified": max_modification_datetime,
                        }
                    )

        if fy_mod_dates:
            return fy_mod_dates
        else:
            print(f"No Parquet files found in the directory: {directory_path}")
            return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []


# COMMAND ----------

def path_exists(path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if "java.io.FileNotFoundException" in str(e):
            return False
        raise e


# COMMAND ----------

function_map = {
    "to_date": (lambda column, expression: f.to_date(column), True),
    "expr": (lambda column, expression: f.expr(expression), False),
    "concat": (lambda column, *args: f.concat(column, *args), True),
    "instr": (lambda column, substr: f.instr(column, substr), True),
    "left": (lambda column, length: f.substring(column, 1, length), True),
    "substring": (
        lambda column, start, length: f.substring(column, start, length),
        True,
    ),
}

def transformSelectColumns(
    filePaths:list, allColumns: list, transform: dict, hashColumns: list, columnAlias: dict = None
) -> list:
    if columnAlias is None:
        columnAlias = {}
    outColumns = []
    availableColumns = spark.read.parquet(*filePaths).columns
    
    for column in allColumns:
        if column not in availableColumns:
            outColumns.append(lit("").alias(column))
            continue

        c = col(f"e.{column}")
        if column in transform:
            for func, args in transform[column]:
                if func in function_map:
                    func, requiresColumn = function_map[func]
                    if requiresColumn:
                        c = func(c, *args)
                    else:
                        c = func(None, *args)
                else:
                    raise ValueError(
                        f"Function {func} not found in function_map for column {column}"
                    )

        if column in hashColumns:
            c = upper(sha2(concat(lit(SALT), c), 256))

        if isinstance(columnAlias.get(column), list):
            [outColumns.append(c.alias(a)) for a in columnAlias.get(column)]
        else:
            outColumns.append(c.alias(columnAlias.get(column, column)))
    return outColumns

# COMMAND ----------

# Get providers organisations
def getProvidersOrgs() -> DataFrame:
    providers_path = "abfss://"
    return (
        spark.read.parquet(providers_path)
        .select(col("Organisation_Code").alias("OrgCode"))
        .where("Region_Code='Y59'")
        # .where("Organisation_Code like 'RN5%'")
        .dropDuplicates(["OrgCode"])
    )

# Get GP organisations
def getGPOrgs() -> DataFrame:
    gp_path = "abfss://"
    return (
        spark.read.parquet(gp_path)
        .select(col("GP_Code").alias("OrgCode"))
        .where("GP_Region_Code='Y59'")
        # .where("1=0")
        .dropDuplicates(["OrgCode"])
    )

# COMMAND ----------

def getParentDatasetKeys(
    paths: list,
    providers: DataFrame,
    gp: DataFrame,
    keyColumns: list,
    filterCondition: str,
) -> DataFrame:

    df = (
        spark.read.parquet(*paths)
        .alias("e")
        .join(
            providers.alias("p"),
            (col("e.Der_Provider_Code") == col("p.OrgCode")),
            "left",
        )
        .join(
            gp.alias("g"),
            (col("e.GP_Practice_Code") == col("g.OrgCode")),
            "left",
        )
        .select(*keyColumns, f"e.{FY_COLUMN_NAME}")
        .filter("(p.OrgCode IS NOT NULL OR g.OrgCode IS NOT NULL)")
    )

    if filterCondition is not None:
        df = df.filter(filterCondition)

    return df

# COMMAND ----------

def getCurrentDatasetHash(
    paths: list,
    keysJoin: DataFrame,
    keyColumns: list,
    hashColumns: list,
    filterCondition: str,
) -> DataFrame:

    aliasedKeyColumns = [col(f"e.{c}") for c in keyColumns]

    df = (
        spark.read.parquet(*paths)
        .alias("e")
        .join(keysJoin.alias("k"), keyColumns, "inner")
        .select(
            *aliasedKeyColumns,
            f"e.{FY_COLUMN_NAME}",
            sha2(concat_ws("~", *hashColumns), 256).alias("HashRecord"),
        )
    )

    if filterCondition is not None:
        df = df.filter(filterCondition)

    groupDf = df.groupBy(*aliasedKeyColumns, FY_COLUMN_NAME).agg(
        sha2(
            concat_ws("~", f.sort_array(f.collect_list(col("HashRecord")))), 256
        ).alias("Hash"),
        f.count(lit(1)).alias("hash_count"),
    )
    return groupDf


# COMMAND ----------

def extractCurrentDataset(
    paths: list,
    keysJoin: DataFrame,
    keyColumns: list,
    allColumns: list,
    filterCondition: str,
) -> DataFrame:

    df = (
        spark.read.parquet(*paths)
        .alias("e")
        .join(keysJoin.alias("k"), keyColumns, "semi")
        .select(*allColumns)
    )

    if filterCondition is not None:
        df = df.filter(filterCondition)

    return df


# COMMAND ----------

def getPreviousHashDataSet(
    path: str, schema: StructType, extractFY: DataFrame, joinType: str
) -> DataFrame:
    try:
        prev_Hash = spark.read.schema(schema).parquet(path)
        prev_Hash = prev_Hash.join(extractFY, FY_COLUMN_NAME, joinType)
    except Exception as e:
        prev_Hash = spark.createDataFrame([], schema)
        prev_Hash.write.mode("overwrite").parquet(path)
    return prev_Hash


# COMMAND ----------

def calculateDatasetChanges(
    prevDf: DataFrame, currDf: DataFrame, keyColumns: list, outputColumns: list
) -> DataFrame:
    joinColumns = keyColumns + ["Hash"]

    return (
        currDf.alias("c")
        .join(prevDf.alias("p"), joinColumns, "left_anti")
        .select(*outputColumns)
    )


# COMMAND ----------

def split_column_alias(columns: list) -> tuple[list[str], dict[str, str]]:
    columnNames = []
    columnAlias = {}

    for c in columns:
        if isinstance(c, list):
            if c[0] == "":
                columnNames.append(c[1])
            else:
                columnNames.append(c[0])
                columnAlias[c[0]] = c[1]
        else:
            columnNames.append(c)

    return columnNames, columnAlias

def process_entities(entities, parent_funcs=None, child_funcs=None, shared_state=None):
    if parent_funcs is None:
        parent_funcs = []

    if child_funcs is None:
        child_funcs = []

    if shared_state is None:
        shared_state = {}

    for parent, details in entities.items():
        parent_results = {}
        config = {}
        config["sourceExtractPaths"] = details.get("extractPaths", [])
        config["prevExtractHashPath"] = f"{BASE_EXTRACT_PATH}/{parent}_Previous"
        config["currExtractHashPath"] = f"{BASE_EXTRACT_PATH}/{parent}_Current"
        config["extractOutPath"] = f"{BASE_EXTRACT_PATH}/{parent}_Out"
        config["extractCSVPath"] = f"{BASE_EXTRACT_PATH}/{parent}_CSV"
        config["extractColumns"], config["extractAliases"] = split_column_alias(config_data[parent]["columns"])
        config["keyColumns"] = config_data[parent]["keys"]
        config["hashColumns"] = config_data[parent]["encrypt"]
        config["transformColumns"] = config_data[parent]["transform"]
        config["filterData"] = config_data[parent]["filter"]
        config["keysPath"] = f"{BASE_EXTRACT_PATH}/{parent}_keys"
        config["extractKeysPath"] = f"{BASE_EXTRACT_PATH}/{parent}_extractkeys"

        for parent_func in parent_funcs:
            result = parent_func(parent, None, details, parent_results, shared_state, config)
            if result is not None:
                parent_results.update(result)

        for child, child_details in details.get("children", {}).items():
            child_results = parent_results.copy()
            child_config = {}
            child_config["sourceExtractPaths"] = child_details.get("extractPaths", [])
            child_config["prevExtractHashPath"] = f"{BASE_EXTRACT_PATH}/{child}_Previous"
            child_config["currExtractHashPath"] = f"{BASE_EXTRACT_PATH}/{child}_Current"
            child_config["extractOutPath"] = f"{BASE_EXTRACT_PATH}/{child}_Out"
            child_config["extractCSVPath"] = f"{BASE_EXTRACT_PATH}/{child}_CSV"
            child_config["extractColumns"], child_config["extractAliases"] = split_column_alias(config_data[child]["columns"])
            child_config["keyColumns"] = config_data[child]["keys"]
            child_config["hashColumns"] = config_data[child]["encrypt"]
            child_config["transformColumns"] = config_data[child]["transform"]
            child_config["filterData"] = config_data[child]["filter"]
            child_config["keysPath"] = f"{BASE_EXTRACT_PATH}/{parent}_keys"
            child_config["extractKeysPath"] = f"{BASE_EXTRACT_PATH}/{parent}_extractkeys"

            for child_func in child_funcs:
                result = child_func(child,parent,child_details,child_results,shared_state,child_config)
                if result is not None:
                    child_results.update(result)


# COMMAND ----------

def process_extract_folders(entity, parent, details, results, state, config):
    print(f"Getting current folders for: {entity}")
    mod_time = get_latest_modification_time(entity, details["basePath"])

    if mod_time:
        state["curr_FYFolders"] = state["curr_FYFolders"].union(
            spark.createDataFrame(mod_time, fy_folders)
        )

def set_extract_paths(entity, parent, details, results, state, config):
    print(f"Setting extracts paths for: {entity}")
    sourceExtractPath = details["basePath"]
    extactPaths = [sourceExtractPath + "/" + f for f in extract_FYFolders if path_exists(sourceExtractPath + "/" + f)]

    if parent is None:
        state["entities"][entity]["extractPaths"] = extactPaths
    else:
        state["entities"][parent]["children"][entity]["extractPaths"] = extactPaths


# COMMAND ----------

def process_parent_keys(entity, parent, details, results, state, config):
    print(f"Processing keys for parent: {entity}")

    keyExtractColumns = transformSelectColumns(config["sourceExtractPaths"], config["keyColumns"], config["transformColumns"], [], None)

    getParentKeys = getParentDatasetKeys(
        config["sourceExtractPaths"],
        state["providers"],
        state["gp"],
        keyExtractColumns,
        config["filterData"],
    )
    getParentKeys.distinct().write.mode("overwrite").parquet(config["keysPath"])


# COMMAND ----------

def process_current_hash(entity, parent, details, results, state, config):
    print(f"Processing current hashes for: {entity}")

    if parent is None:
        print(f"Read and cache keys for: {entity}")
        keyset = entity
        state[keyset] = spark.read.parquet(config["keysPath"])
        state[keyset].count()
    else:
        keyset = parent

    columnsToHash = [c for c in config["extractColumns"] if c not in config["keyColumns"]]
    hashExtractColumns = transformSelectColumns(config["sourceExtractPaths"], columnsToHash, config["transformColumns"], [], None)

    currentHash = getCurrentDatasetHash(
        config["sourceExtractPaths"],
        state[keyset],
        config["keyColumns"],
        hashExtractColumns,
        config["filterData"],
    )
    currentHash.write.mode("overwrite").partitionBy(FY_COLUMN_NAME).parquet(config["currExtractHashPath"])


# COMMAND ----------

def process_hash_changes(entity, parent, details, results, state, config):
    print(f"Calculate changed hashes for: {entity}")

    if parent is None:
        keyset = entity
        state[keyset] = []
    else:
        keyset = parent

    currentHash = spark.read.schema(currentHashSchema).parquet(config["currExtractHashPath"])
    previousHash = getPreviousHashDataSet(config["prevExtractHashPath"],currentHashSchema,extract_FinanicalYears,"inner")

    tmp_extractKeys = calculateDatasetChanges(previousHash, currentHash, config["keyColumns"], config["keyColumns"])

    state[keyset].append(tmp_extractKeys)


def reduce_keys(entity, parent, details, results, state, config):
    print(f"Reduce keys for: {entity}")
    extractKeys = reduce(lambda df1, df2: df1.union(df2), state[entity]).distinct()
    extractKeys.write.mode("overwrite").parquet(config["extractKeysPath"])


# COMMAND ----------

def extract_data_changes(entity, parent, details, results, state, config):
    print(f"Extracting data for: {entity}")

    if parent is None:
        print(f"Read and cache keys for: {entity}")
        keyset = entity
        state[keyset] = spark.read.parquet(config["extractKeysPath"])
        state[keyset].count()
    else:
        keyset = parent

    extractColumnsHash = transformSelectColumns(config["sourceExtractPaths"],config["extractColumns"],config["transformColumns"],config["hashColumns"],config["extractAliases"])
    extractCurrent = extractCurrentDataset(config["sourceExtractPaths"],state[keyset],config["keyColumns"],extractColumnsHash,config["filterData"])

    controlFileEntry[entity] = {
        "nRecordCount": extractCurrent.count(),
        "cDatum": CONTROL_FILE_DATUM,
        "dPeriodStart": datetime(1970, 4, 1),
        "dPeriodEnd": datetime(2999, 1, 1),
    }

    extractCurrent.write.mode("overwrite").parquet(config["extractOutPath"])


# COMMAND ----------


def extract_csv(entity, parent, details, results, state, config):
    print(f"Extracting csv for: {entity}")

    csvOut = spark.read.parquet(config["extractOutPath"])

    total_rows = controlFileEntry[entity]["nRecordCount"]
    numparts = max(1, int(total_rows / TARGET_ROWS) + 1)
    # Write the transformed DataFrame to CSV
    csvOut.repartition(numparts).write.options(
        header=True,
        delimiter="|",
        lineSep="\n",
        encoding="windows-1252",
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        timestampntzformat="yyyy-MM-dd HH:mm:ss",
        compression="gzip",
        emptyValue="",
    ).mode("overwrite").csv(config["extractCSVPath"])


# COMMAND ----------


def move_extract_csv(entity, parent, details, results, state, config):
    print(f"Moving extract CSV for: {entity}")
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    controlFileEntry[entity]["cExtractFileName"] = []

    csvFiles = dbutils.fs.ls(config["extractCSVPath"])
    csvParts = [f.path for f in csvFiles if f.name.endswith(".csv.gz")]
    for i, file in enumerate(csvParts, start=1):
        csvName = f"{BASE_EXTRACT_PATH}/{entity}_part{i}_{now}.csv.gz"
        dbutils.fs.mv(file, csvName)
        controlFileEntry[entity]["cExtractFileName"].append(Path(csvName).name)

    dbutils.fs.rm(config["extractCSVPath"], recurse=True)
    dbutils.fs.rm(config["extractOutPath"], recurse=True)


# COMMAND ----------


def storeCurrentParentKeys(entity, parent, details, results, state, config):
    def getDeleteFilename(entity, part, now):
        before, sep, after = entity.partition("_")
        return f"{before}Deletes{sep}{after}_part{i}_{now}.csv.gz"

    print(f"Storing key/hash: {entity}")
    prevExtractHashPath = config["prevExtractHashPath"]
    currExtractHashPath = config["currExtractHashPath"]
    outExtractHashPath = f"{prevExtractHashPath}New"

    deleteKeysPath = f"{BASE_EXTRACT_PATH}/{entity}Deletes_Out"
    deleteKeysPathCSV = f"{BASE_EXTRACT_PATH}/{entity}Deletes_CSV"

    previousHashFY = getPreviousHashDataSet(
        prevExtractHashPath, currentHashSchema, extract_FinanicalYears, "left_anti"
    )
    currentHashFY = (
        spark.read.schema(currentHashSchema)
        .parquet(currExtractHashPath)
        .select(*previousHashFY.columns)
    )

    savedHash = currentHashFY.union(previousHashFY)
    nRecordCount = savedHash.count()
    savedHash.write.mode("overwrite").partitionBy(FY_COLUMN_NAME).parquet(
        outExtractHashPath
    )
    
    deleteColumns = transformSelectColumns(config["sourceExtractPaths"], config["keyColumns"], config["transformColumns"], config["hashColumns"], config["extractAliases"])

    total_rows = nRecordCount
    numparts = max(1, int(total_rows / TARGET_ROWS) + 1)
    savedHash.alias("e").select(*deleteColumns).dropDuplicates().repartition(
        numparts
    ).write.options(
        header=True,
        delimiter="|",
        lineSep="\n",
        encoding="windows-1252",
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        timestampntzformat="yyyy-MM-dd HH:mm:ss",
        compression="gzip",
        emptyValue="",
    ).mode(
        "overwrite"
    ).csv(
        deleteKeysPath
    )

    controlFileEntry[f"{entity}Deletes"] = {
        "nRecordCount": nRecordCount,
        "cDatum": CONTROL_FILE_DATUM,
        "dPeriodStart": datetime(1970, 4, 1),
        "dPeriodEnd": datetime(2999, 1, 1),
        "cExtractFileName": [],
    }

    csvFiles = dbutils.fs.ls(deleteKeysPath)
    csvParts = [f.path for f in csvFiles if f.name.endswith(".csv.gz")]
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i, file in enumerate(csvParts, start=1):
        csvName = f"{BASE_EXTRACT_PATH}/{getDeleteFilename(entity, i, now)}"
        dbutils.fs.mv(file, csvName)
        controlFileEntry[f"{entity}Deletes"]["cExtractFileName"].append(
            Path(csvName).name
        )

    dbutils.fs.rm(deleteKeysPathCSV, recurse=True)
    dbutils.fs.rm(deleteKeysPath, recurse=True)


def storeCurrentChildKeys(entity, parent, details, results, state, config):
    print(f"Storing key/hash: {entity}")
    prevExtractHashPath = config["prevExtractHashPath"]
    currExtractHashPath = config["currExtractHashPath"]
    outExtractHashPath = f"{prevExtractHashPath}New"

    previousHashFY = getPreviousHashDataSet(
        prevExtractHashPath, currentHashSchema, extract_FinanicalYears, "left_anti"
    )
    currentHashFY = (
        spark.read.schema(currentHashSchema)
        .parquet(currExtractHashPath)
        .select(previousHashFY.columns)
    )

    savedHash = currentHashFY.union(previousHashFY)
    savedHash.write.mode("overwrite").partitionBy(FY_COLUMN_NAME).parquet(
        outExtractHashPath
    )


# COMMAND ----------


def entity_cleanup(entity, parent, details, results, state, config):
    print(f"Cleanup: {entity}")

    prevExtractHashPath = config["prevExtractHashPath"]
    currExtractHashPath = config["currExtractHashPath"]
    outExtractHashPath = f"{prevExtractHashPath}New"
    keysPath = config["keysPath"]
    extractKeysPath = config["extractKeysPath"]

    if path_exists(currExtractHashPath):
        dbutils.fs.rm(currExtractHashPath, recurse=True)
    else:
        raise Exception(f"Path {currExtractHashPath} does not exist")

    if path_exists(prevExtractHashPath):
        dbutils.fs.rm(prevExtractHashPath, recurse=True)
    else:
        raise Exception(f"Path {prevExtractHashPath} does not exist")

    if path_exists(outExtractHashPath):
        dbutils.fs.mv(outExtractHashPath, prevExtractHashPath, recurse=True)
    else:
        raise Exception(f"Path {outExtractHashPath} does not exist")

    if path_exists(keysPath) and parent is None:
        dbutils.fs.rm(keysPath, recurse=True)

    if path_exists(extractKeysPath) and parent is None:
        dbutils.fs.rm(extractKeysPath, recurse=True)


# COMMAND ----------


def writeControlFile(controlFileName):
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    extractOutPath = f"{BASE_EXTRACT_PATH}/{controlFileName}Control_Out"

    controlFileEntry_df = spark.createDataFrame(
        controlFileEntry.values(), controlFileEntrySchema
    )

    controlFileEntry_df = controlFileEntry_df.withColumn(
        "cExtractFileName", explode(col("cExtractFileName"))
    )

    controlFileEntry_df.coalesce(1).write.options(
        header=True,
        delimiter="|~|",
        lineSep="\r\n",
        encoding="windows-1252",
        dateFormat="yyyy-MM-dd",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
        timestampntzformat="yyyy-MM-dd HH:mm:ss",
        emptyValue="",
    ).mode("overwrite").csv(extractOutPath)

    ctlFiles = dbutils.fs.ls(extractOutPath)
    ctlParts = [f.path for f in ctlFiles if f.name.endswith(".csv")][0]
    ctlName = f"{BASE_EXTRACT_PATH}/{controlFileName}_{now}.ctl"

    dbutils.fs.mv(ctlParts, ctlName)
    dbutils.fs.rm(extractOutPath, recurse=True)