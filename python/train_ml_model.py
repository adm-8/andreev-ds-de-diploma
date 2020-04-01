'''
    30.03.2020 Andreev Dima
    Telegram: @dslads

    Поскольку результат выдачи предварительнго одобрения в нашем случае линейно зависит от ЗП, Запрашиваемой суммы и периода кредита
    мы не станем заморачиваться с подбором модели и возьмём старый добрый LogisticRegression из библиотеки SKLearn
    
    С подбором гипепараметров тоже заморачиваться не станем ибо:
        1) у нас синтетические данные
        2) у нас не стоит задачи сделать чёткую ML модель
    
    Для работы с данными воспользуемся всеми любимой библиотекой Pandas
    
'''

import os
import pickle
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from statistics import mean

MODEL_FILE_NAME = '_logreg_clf_model.py'

# формируем пути
root_path =  os.path.join(os.getcwd(), '..')
data_path = os.path.join(root_path, 'data', 'train.csv')

# читаем данные  
raw_data = pd.read_csv(data_path)

# представим, что наши DS разведали и поняли, что на результат влияет только ЗП, Запрашиваемой суммы и периода кредита
X = raw_data[['SALARY','LOAN_AMOUNT','PERIOD']]
y = raw_data['TARGET']

# разобьем наши данные на обучающие и тестовые
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# обучим нашу модель:
print("Training model...")
clf = LogisticRegression(random_state=0, solver='liblinear').fit(X_train,y_train)
print("Model trained...")

# Оценим нашу модель
score = mean(cross_val_score(clf, X, y, cv=5, scoring='f1'))
print("\n\n\n")
print("The average model score is {0}".format(score))

# если среднеезначение точности модеди больше 94%, будем считать, что модель полходящая
if score < 0.94:
    print("\n\n**********\n")
    print("Not enough score for saving the model!")
    print("\n\n\n\n**********\n")
else:
    print("\n\n**********\n")
    print("Great model! Let's save it!")
    pickle.dump(clf, open(MODEL_FILE_NAME, 'wb'), pickle.HIGHEST_PROTOCOL)
    print("\n\n**********\n")
    
