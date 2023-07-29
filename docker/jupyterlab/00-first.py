from delta import *
from pyspark.sql import SparkSession

builder = (SparkSession.builder
           .appName("data-eng-cookbook")
           .master("spark://spark-master:7077")
           .config("spark.executor.memory", "512m")   
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


get_ipython().run_line_magic('load_ext', 'sparksql_magic')
get_ipython().run_line_magic('config', 'SparkSql.limit=20')