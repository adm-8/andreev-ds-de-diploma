'''
    31.03.2020 Andreev Dima
    Telegram: @dslads
    
    Читаем данные из файла request.csv по кускам, склеиваем все поля в одну строку и отправляем данные в топик кафки OptyInputTopic 
    
'''
import os
import time
from pyspark.sql import SparkSession

# настройки для соединения с кафкой
KAFKA_HOST = '34.71.139.131' 
KAFKA_PORT = '9092'
KAFKA_TOPIC = 'OptyInputTopic'

# кол-во итераций
ITER = 1000
FRACT = 1 / ITER

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

for x in range(ITER):    
    data = raw_data.sample(fraction=FRACT, withReplacement=False)
    print("Data prepared!... Sending data to Kafka...")
    
    data.selectExpr("CAST(UUID AS STRING) as key", "REGION || ',' || JOB_TITLE || ',' || CAST(SALARY AS STRING) || ',' || CAST(LOAN_AMOUNT AS STRING) || ',' || CAST(PERIOD AS STRING)  as value") \
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
      .option("topic", KAFKA_TOPIC) \
      .save()

    print("Batch {0} was sended successfully!".format(x))
    time.sleep(1) # Sleep for 1 second

#spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 D:\_git\andreev-ds-de-diploma\python\kafka_producer.py    