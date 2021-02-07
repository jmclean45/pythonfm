import random
import string
from calendar import monthrange
import datetime
import psycopg2

def connect():
    conn = psycopg2.connect(user = "postgres", password = "near476", host = "localhost", port = "5432", database = "Practice")
    cur = conn.cursor()
    return conn

def random_string(num=7):
    letters = string.ascii_uppercase
    return str( ''.join(random.choice(letters) for i in range(6)) )[:num]

def random_int():
    x = random.randint(222, 999)
    return x

def get_1st_and_lst_day(year,month):
    first_date= datetime.date(year, month, 1)
    last_date = first_date.replace(day=monthrange(first_date.year,first_date.month)[1])
    return str(first_date), str(last_date)

def get_final_dates(month,year):
    final_dates=[]
    final_dates.append((str(datetime.date(year,month,1)),str(datetime.date(year,month,10))))
    final_dates.append((str(datetime.date(year,month,11)),str(datetime.date(year,month,20))))
    final_dates.append((str(datetime.date(year,month,21)),str(datetime.date(year,month,28))))

    return final_dates

def checkdic():
    dic= {'ln':23,'l2':33,'l4':55}
    return dic
