from pyspark.sql import SparkSession 
from pyspark.sql.functions import * 
from pyspark.sql.types import *
# Create a new SparkSession
spark = (SparkSession
         .builder
         .appName("optimize-data-shuffles")
         .master("spark://spark-master:7077")
         .config("spark.executor.memory", "512m")
         .getOrCreate())

# Create some sample data frames
# A large data frame with 1 million rows and two columns: id and value
large_df = (spark.range(0, 1000000)
            .withColumn("date", date_sub(current_date(), (rand() * 365).cast("int")))
            .withColumn("age", (rand() * 100).cast("int"))
            .withColumn("salary", 100*(rand() * 100).cast("int"))
            .withColumn("gender", when((rand() * 2).cast("int") == 0, "M").otherwise("F"))
            .withColumn("grade", 
                        when((rand() * 5).cast("int") == 0, "IC")
                        .when((rand() * 5).cast("int") == 1, "IC-2")
                        .when((rand() * 5).cast("int") == 2, "M1")
                        .when((rand() * 5).cast("int") == 3, "M2")
                        .when((rand() * 5).cast("int") == 4, "IC-3")
                        .otherwise("M3")))
large_df.show(5)