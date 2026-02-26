from pyspark import pipelines as dp
from pyspark.sql import functions as F

def city_silver():
    df_bronze=spark.read.table("transportation.bronze.city")
