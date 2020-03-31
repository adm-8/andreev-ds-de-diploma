# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 D:\_git\andreev-ds-de-diploma\python\kafka_consumer.py


import pickle
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType
from pyspark.sql.functions import udf


# настройки для соединения с кафкой
KAFKA_HOST = '34.71.139.131' 
KAFKA_PORT = '9092'
KAFKA_INPUT_TOPIC = 'OptyInputTopic'
KAFKA_OUTPUT_TOPIC = 'OptyOutputTopic'

MODEL_FILE_NAME = '_logreg_clf_model.py' # сама модель создается в файле train_ml_model.py


# читаем сохраненную модель 
with open(MODEL_FILE_NAME, "rb") as f:
    clf_loaded = pickle.load(f)

    # Определяем функцию применения ML модели
    def get_prediction(input_string, clf = clf_loaded):    
        # читаем данные
        input_str_array = np.array(input_string.split(',')[2:5]) 
        # конвертируем их в удбоваримый для модели float array
        float_array = input_str_array.astype(np.float)
        # получаем предсказание
        pred_row = clf.predict([float_array])[0]
        # возвращаем результат
        return int(pred_row)
     
    # определяем UDF-функцию для использования в контексте спарка
    udf_get_prediction = udf(lambda x: get_prediction(x), IntegerType())
    
    
    # получаем спарковую сессию
    spark = SparkSession \
        .builder \
        .appName("AndreevDS-DE-Diploma-Kafka-Consumer") \
        .getOrCreate()
        
    print("Spark context started")
    
    spark.udf.register("udf_get_prediction", udf_get_prediction)
    print("\n\n\n - - - udf_get_prediction registred succsessfully - - - \n\n\n")

    

    #val = get_prediction("Moscow,Data Analytic,112309.0,995956.0,48.0") # , clf
    #print(val)
    
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
      .option("subscribe", KAFKA_INPUT_TOPIC) \
      .load()
      
    df.selectExpr("CAST(key AS STRING) as key", "udf_get_prediction(CAST(value AS STRING)) as value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "{0}:{1}".format(KAFKA_HOST, KAFKA_PORT)) \
    .option("topic", KAFKA_OUTPUT_TOPIC) \
    .start() \
    .awaitTermination()

       
    
    #.option("checkpointLocation", checkpointLocation) \
    
   
    

    

    

    
    