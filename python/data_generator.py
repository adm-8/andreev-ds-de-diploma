from random import choice

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

for x in range(10000):
    print(get_opty())
    