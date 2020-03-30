#spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 D:\_git\andreev-ds-de-diploma\python\kafka_producer.py

KAFKA_HOST = '34.66.73.57'
KAFKA_PORT = '9092'
KAFKA_TOPIC = 'OptyInputTopic'

import os
from pyspark.sql import SparkSession

# получаем спарковую сессию
spark = SparkSession \
    .builder \
    .appName("AndreevDS-DE-Diploma-Kafka-Producer") \
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
    StructField("PERIOD", IntegerType(), True)
])

# формируем путь к файлу с данными
data_path = os.path.join(os.getcwd(), '..', 'data', 'request.csv')

# читаем данные 
raw_data = spark.read.csv(data_path,header=True,schema=schema) \
    .selectExpr("UUID","REGION","JOB_TITLE","SALARY", "LOAN_AMOUNT", "PERIOD")

data = raw_data.sample(fraction=0.0003, withReplacement=False)

print("Data prepared!... Sending to Kafka")

data.selectExpr("CAST(UUID AS STRING) as key", "CAST(REGION AS STRING) as value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
  .option("topic", KAFKA_TOPIC) \
  .save()

print("Kafka Producer successfully started!")