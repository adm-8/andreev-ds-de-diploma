import os
from pyspark.sql import SparkSession
import pyspark.sql.functions
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf

# настройки для соединения с кафкой
KAFKA_HOST = '34.71.139.131' 
KAFKA_PORT = '9092'
KAFKA_INPUT_TOPIC = 'OptyInputTopic'
KAFKA_OUTPUT_TOPIC = 'OptyOutputTopic'


# формируем пути
data_root_path =  os.path.join(os.getcwd(), '..', 'data')
parquet_path = os.path.join(data_root_path, '')
checkpointLocation = os.path.join(data_root_path, 'checkpointOptyOutput')

# Определяем функцию получения значений из CSV-стринги
def get_from_csv(input_string, idx = 0, sep = ','):    
    '''
        Функция получает на вход строку input_string с данными, разделенными символом sep и возвращает значение idx "колонки"
    '''
    # читаем данные
    return str(input_string.split(sep)[idx]) 
 
# определяем UDF-функцию для использования в контексте спарка
udf_get_from_csv = udf(lambda s, i: get_from_csv(s,i), StringType())

# получаем спарковую сессию
spark = SparkSession \
    .builder \
    .appName("AndreevDS-DE-Diploma-Kafka-Consumer-Join") \
    .getOrCreate()
    
print("Spark context started")

# регистрируем нашу функцию
spark.udf.register("udf_get_from_csv", udf_get_from_csv)
print("\n\n\n - - - udf_get_from_csv registred succsessfully - - - \n\n\n")

# -----------------------------
# читаемм данные по КЗ из кафки
# -----------------------------

df_opty_in = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
  .option("subscribe", KAFKA_INPUT_TOPIC) \
  .load()

# причёсываем наши данные по КЗ 
df_opty_in.selectExpr( \
        "CAST(key AS STRING) as t1_key" \
        ,"CAST(udf_get_from_csv(CAST(value AS STRING), 0) AS STRING) as t1_region" \
        ,"CAST(udf_get_from_csv(CAST(value AS STRING), 1) AS STRING) as t1_job_title" \
        ,"CAST(udf_get_from_csv(CAST(value AS STRING), 2) AS STRING) as t1_salary" \
        ,"CAST(udf_get_from_csv(CAST(value AS STRING), 3) AS STRING) as t1_loan_amount" \
        ,"CAST(udf_get_from_csv(CAST(value AS STRING), 4) AS STRING) as t1_period") 
  
    
# ----------------------------------------    
# читаемм данные с предсказаниями из кафки
# ----------------------------------------
    
df_opty_out = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
  .option("subscribe", KAFKA_OUTPUT_TOPIC) \
  .load().selectExpr("CAST(key AS STRING) as t2_key", "CAST(value AS STRING) as t2_value") \
    .writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
  
    
'''
# описываем структуру value в топике OptyInputTopic
schema = StructType([
    #StructField("UUID", StringType(), True),
    StructField("REGION", StringType(), True),
    StructField("JOB_TITLE", StringType(), True),
    StructField("SALARY", FloatType(), True),
    StructField("LOAN_AMOUNT", FloatType(), True),
    StructField("PERIOD", IntegerType(), True)
])

    #.outputMode("complete") \



  
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


    
# читаем данные 
raw_data = spark.read.csv(data_path,header=True,schema=schema) \
    .selectExpr("UUID","REGION","JOB_TITLE","SALARY", "LOAN_AMOUNT", "PERIOD", "TARGET")

raw_data.groupBy("REGION").count().show()
    
'''