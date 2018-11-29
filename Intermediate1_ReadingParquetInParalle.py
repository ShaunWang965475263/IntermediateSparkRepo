#Read parquet files in parallel output can only be done with Spark Sql

from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext, SparkConf

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("ParallelRead").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

tbl = sqlContext.read.parquet("output")
tbl.registerTempTable("temp")
tbl.show(200000)
# For some workloads, it is possible to improve performance by either caching data in memory, or by turning on some experimental options.
