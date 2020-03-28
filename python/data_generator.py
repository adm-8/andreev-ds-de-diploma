from random import choice
import os 

regions = ['Москва','Питер','Казань']
job_titles = ['Программист', 'Бухгалтер', 'Экономист']
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
    ret = choice(regions) +","+ choice(job_titles) +","+  str(sal) +","+  str(la) +","+ str(per)
    
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
        
    header_str = 'REGION,JOB_TITLE,SALARY,LOAN_AMOUNT,PERIOD'
    if with_result == True:
        header_str = header_str + ",TARGET"
        file_path =  os.path.join(file_path , "train.csv")
    else:
        file_path = os.path.join(file_path , "request.csv")
    
    f = open(file_path, 'w+')
    f.write(header_str)
    
    for x in range(rec_cnt):
        f.write("\n" + get_opty(with_result))
        
    f.close()
    print("File created")    
    #print(header_str)
    
make_csv_file(100000, True)
make_csv_file(100000, False)

print(os.path.join(os.getcwd(), '..', 'data'))
