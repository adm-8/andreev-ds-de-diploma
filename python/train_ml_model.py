import os
from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("AndreevDS-DE-Diploma-ML") \
    .getOrCreate()
    
print("Spark context started")

from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

schema = StructType([
    StructField("REGION", StringType(), True),
    StructField("JOB_TITLE", StringType(), True),
    StructField("SALARY", LongType(), True),
    StructField("LOAN_AMOUNT", LongType(), True),
    StructField("PERIOD", IntegerType(), True),
    StructField("TARGET", IntegerType(), True)
])
    
data_path = os.path.join(os.getcwd(), '..', 'data', 'train.csv')

raw_data = spark.read.csv(data_path,header=True,schema=schema) \
    .selectExpr("REGION","JOB_TITLE","SALARY", "LOAN_AMOUNT", "PERIOD", "TARGET")

raw_data.groupBy("REGION").count().show()