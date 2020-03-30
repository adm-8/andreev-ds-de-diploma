# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 D:\_git\andreev-ds-de-diploma\python\train_ml_model.py

import os
from pyspark.sql import SparkSession

# получаем спарковую сессию
spark = SparkSession \
    .builder \
    .appName("AndreevDS-DE-Diploma-ML") \
    .getOrCreate()
    
print("Spark context started")

# описываем структуру файла
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

schema = StructType([
    StructField("UUID", StringType(), True),
    StructField("REGION", StringType(), True),
    StructField("JOB_TITLE", StringType(), True),
    StructField("SALARY", LongType(), True),
    StructField("LOAN_AMOUNT", LongType(), True),
    StructField("PERIOD", IntegerType(), True),
    StructField("TARGET", IntegerType(), True)
])
    
# формируем путь к файлу с данными
data_path = os.path.join(os.getcwd(), '..', 'data', 'train.csv')

# читаем данные 
raw_data = spark.read.csv(data_path,header=True,schema=schema) \
    .selectExpr("UUID","REGION","JOB_TITLE","SALARY", "LOAN_AMOUNT", "PERIOD", "TARGET")

raw_data.groupBy("REGION").count().show()
    
