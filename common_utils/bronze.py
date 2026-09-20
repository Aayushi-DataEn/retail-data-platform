import pyspark.sql.functions as F
from common_utils.logging import get_logger

logger = get_logger("common_utils_bronze")

def read_raw(spark, raw_path, file_format, options = None ):

    """
    For this we are doing this ======

    df = spark.read\
    .format("csv")\
    .option("header","true")\
     .option("inferSchema", "true")\
         .load(raw_path) 
    """
    """ DESCRIPTION: 

    Read raw files from volume

    raw_path.      : "/Volumes/retaildataplatform/bronze/raw_data/sqlserver_customers/load_date=2026-09-07/"

    file_format    : "json", "csv", "delta or parquet"
    options        : key value
    
    returns a dataframe
    """
    logger.info("reading from path: %s", raw_path)
    logger.info("File Format: %s", file_format)

    reader = spark.read.format(file_format)
    for key, value in (options or {}).items():
        reader = reader.option(key, value) 
         #.option("header","true") key and valu in ()
    

    return reader.load(raw_path)
    logger.info("Read Complete and daat stored in Dataframe")




def bronze_ingestor(df, mode, target_table):

    """
    Add audit columns and write in bronze as Delta table

    df          : DataFrame read from raw volume
    target_table: catalog.schema.table
    mode        : "overwrite" or "append"

    return the no. of rows written
    """

    logger.info("Adding audit columns in Dataframe")

    df = (
        df.withColumn("last_update_ts", F.current_timestamp())
          .withColumn("file_path", F.col("_metadata.file_path"))
    )

    logger.info("Writing Delta Table: %s", target_table)
    logger.info("Selected mode is : %s", mode)

    df.write.format("delta").mode(mode).saveAsTable(target_table)

    return df.count()