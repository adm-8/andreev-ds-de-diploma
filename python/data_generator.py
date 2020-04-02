#! /usr/bin/env python
# -*- coding: utf-8 -*-

'''
    29.03.2020 Andreev Dima
    Telegram: @dslads
    
    Генерируем два CSV файла с данными по Кредитным заявкам:
    - train.csv - на нем обучается ML модель в скрипте train_ml_model.py
    - request.csv - этот файл используется для отправки данных в топик кафки в скрипте kafka_producer
    
'''

from random import choice
import os 
import uuid

regions = ['Moscow','Kazan','Saint Petersburg']
job_titles = ['Programmer', 'Data Analytic', 'Business Analytic']
salary = [112309, 89450, 96231, 75614, 157495, 189242, 210561, 137245]
loan_ammount = range(192540, 2000000, 13852)
preiod = range(12, 120, 12)


def get_opty(with_result = True):
    
    # отношение ежемесячного платежа к ЗП для получения положительного решения 
    true_percent = 0.3
    
    # Генерим рандомные данные 
    sal = choice(salary)
    la = choice(loan_ammount)
    per = choice(preiod)
    
    # Рассчитываем соотношение ежемесячного платежа к ЗП
    req_percent = (la / per) / sal
    
    # готовим tuple для return
    ret = str(uuid.uuid4()) + "," + choice(regions) +","+ choice(job_titles) +","+  str(sal) +","+  str(la) +","+ str(per)
    
    # если помимо данных для запроса КЗ нужно вернуть и решение 
    if with_result == True:        
        # Если соотношение запрошенных денег к ЗП меньше порога, то даем положительный результат 
        if req_percent < true_percent:
            return ret + ',1'
        
        # иначе отрицательный
        else:         
            return ret + ',0'
    # иначе возвращаем просто данные для запроса КЗ
    else:
        return ret


def make_csv_file(rec_cnt, with_result):
    
    file_path = os.path.join(os.getcwd(), '..', 'data')
        
    header_str = 'UUID,REGION,JOB_TITLE,SALARY,LOAN_AMOUNT,PERIOD'
    if with_result == True:
        header_str = header_str + ",TARGET"
        file_path =  os.path.join(file_path , "train.csv")
    else:
        file_path = os.path.join(file_path , "request.csv")
    
    f = open(file_path, 'w+') #, encoding="utf-8"
    f.write(header_str)
    
    for x in range(rec_cnt):
        f.write("\n" + get_opty(with_result))
        
    f.close()
    print("File created")    
    #print(header_str)
    
make_csv_file(1000000, True)
make_csv_file(200000, False)

print(os.path.join(os.getcwd(), '..', 'data'))
