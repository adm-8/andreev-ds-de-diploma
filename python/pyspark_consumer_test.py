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