from random import choice

regions = ['Москва','Питер','Казань']
job_titles = ['Программист', 'Бухгалтер', 'Экономист']
salary = [112309, 89450, 96231, 75614, 157495, 189242, 210561, 137245]
loan_ammount = [600000, 650000, 700000, 750000, 800000, 850000, 900000, 950000]
preiod = [12, 24, 36, 48, 60]

def get_opty():
    return choice(regions), choice(job_titles), choice(salary), choice(loan_ammount), choice(preiod)

for x in range(10000):
    print(get_opty())