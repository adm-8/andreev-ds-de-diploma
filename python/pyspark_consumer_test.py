'''
spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5
spark-submit \
--jars org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.5 \
D:\_git\andreev-ds-de-diploma\python\pyspark_consumer_test.py


spark-submit --jars C:\Users\user\.ivy2\jars\org.apache.spark_spark-streaming-kafka-0-10_2.11-2.4.5.jar D:\_git\andreev-ds-de-diploma\python\pyspark_consumer_test.py


'''


from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()
    

    
# Subscribe to 1 topic
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "34.68.195.254:9092") \
  .option("subscribe", "testTopic") \
  .load()
  
df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")



print("THAAAAAAATS GREAT!!!! ")