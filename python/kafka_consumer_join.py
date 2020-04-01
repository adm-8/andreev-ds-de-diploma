import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

# настройки для соединения с кафкой
KAFKA_HOST = '34.71.139.131' 
KAFKA_PORT = '9092'
KAFKA_INPUT_TOPIC = 'OptyInputTopic'
KAFKA_OUTPUT_TOPIC = 'OptyOutputTopic'


# формируем пути
data_root_path =  os.path.join(os.getcwd(), '..', 'data')
parquet_path = os.path.join(data_root_path, '')
checkpointLocation = os.path.join(data_root_path, 'checkpointOptyOutput')

# получаем спарковую сессию
spark = SparkSession \
    .builder \
    .appName("AndreevDS-DE-Diploma-Kafka-Consumer-Join") \
    .getOrCreate()
    
print("Spark context started")

# читаемм данные по КЗ из кафки
df_opty_in = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
  .option("subscribe", KAFKA_INPUT_TOPIC) \
  .load()

df_opty_in.selectExpr("CAST(key AS STRING) as t1_key", "CAST(value AS STRING) as t1_value") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start() \
    .awaitTermination()
    
'''
# читаемм данные с предсказаниями из кафки
df_opty_out = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
  .option("subscribe", KAFKA_OUTPUT_TOPIC) \
  .load()
  
df = df_opty_out.join(df_opty_in, $"mainKey" === $"joinedKey")
  
# применяем нашу ML функцию и пишем результат в соседнюю очередь в кафку


'''



#print(clf.predict(X_test))
#print(X_train)
#print(y_train)


'''
# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 D:\_git\andreev-ds-de-diploma\python\train_ml_model.py


.writeStream \
.format("csv") \
.option("checkpointLocation", checkpointLocation) \
.start() \
.awaitTermination()
 




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
    
# читаем данные 
raw_data = spark.read.csv(data_path,header=True,schema=schema) \
    .selectExpr("UUID","REGION","JOB_TITLE","SALARY", "LOAN_AMOUNT", "PERIOD", "TARGET")

raw_data.groupBy("REGION").count().show()
    
'''