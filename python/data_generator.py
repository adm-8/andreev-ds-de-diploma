from random import choice

regions = ['Москва','Питер','Казань']
job_titles = ['Программист', 'Бухгалтер', 'Экономист']
salary = [112309, 89450, 96231, 75614, 157495, 189242, 210561, 137245]
loan_ammount = range(192540, 2000000, 13852)
preiod = range(12, 120, 12)

def get_opty(with_result = True):
    if with_result == True:
        sal = choice(salary)
        la = choice(loan_ammount)
        per = choice(preiod)
        req_percent = (la / per) / sal
        true_percent = 0.35
        res = 0
        if req_percent < true_percent:
            res = 1         
        return choice(regions), choice(job_titles), sal, la, per, req_percent, res
    
    else:
        return choice(regions), choice(job_titles), choice(salary), choice(loan_ammount), choice(preiod)

for x in range(10000):
    print(get_opty())
    