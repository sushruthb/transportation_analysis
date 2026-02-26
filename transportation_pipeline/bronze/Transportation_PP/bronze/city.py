from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.functions import md5, concat_ws, sha2

#SOURCE_PATH="dbfs:/Volumes/workspace/dbricks/db/csv/transport/data/city/"
SOURCE_PATH="dbfs:/Volumes/transportation/transportation_data/transportation/data/city"
# Destination in catalog PATH="transportation.bronze.city"


@dp.materialized_view(
    name="transportation.bronze.city",
    comment="City data processing",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "souce_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("mode", "PERMISSIVE").option("mergeSchema", "true").option("columnNameOfCorruptRecord","_corrupt_record").load(SOURCE_PATH)

    df = df.withColumn("file_name", col("_metadata.file_path")).withColumn("ingest_datetime", current_timestamp())
    
    return df