'''

    Поскольку результат выдачи предварительнго одобрения в нашем случае линейно зависит от ЗП, Запрашиваемой суммы и периода кредита
    мы не станем заморачиваться с подбором модели и возьмём старый добрый LogisticRegression из библиотеки SKLearn
    
    С подбором гипепараметров тоже заморачиваться не станем ибо:
        1) у нас синтетические данные
        2) у нас не стоит задачи сделать чёткую ML модель
    
    Для работы с данными воспользуемся всеми любимой библиотекой Pandas
    
'''

import os
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from statistics import mean


# формируем пути
root_path =  os.path.join(os.getcwd(), '..')
data_path = os.path.join(root_path, 'data', 'train.csv')

# читаем данные  
raw_data = pd.read_csv(data_path)

# представим, что наши DS разведали и поняли, что на результат влияет только ЗП, Запрашиваемой суммы и периода кредита
X = raw_data[['SALARY','LOAN_AMOUNT','PERIOD']]
y = raw_data['TARGET']

# разобьем наши данные на обучающие и тестовые
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# обучим нашу модель:
print("Training model...")
clf = LogisticRegression(random_state=0).fit(X_train,y_train)
print("Model trained...")

# Оценим нашу модель
score = mean(cross_val_score(clf, X, y, cv=5, scoring='f1'))
print("\n\n\n")
print("The average model score is {0}".format(score))

if score < 0.94:
    print("\n\n**********\n")
    print("Not enough score for saving the model!")
    print("\n\n\n\n**********\n")
else:
    print("\n\n**********\n")
    print("Great model! Let's save it!")
    print("\n\n**********\n")
    


#print(clf.predict(X_test))
#print(X_train)
#print(y_train)


'''
# spark-submit  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 D:\_git\andreev-ds-de-diploma\python\train_ml_model.py

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