import datetime
import psycopg2
from common_util import *


def create_colt_pool(cur, mbs_pool_id):
    sql ='INSERT INTO "COLT_POOL" ("MBS_POOL_ID") VALUES (%s) RETURNING "COLT_POOL_ID"'
    val = (mbs_pool_id,)
    cur.execute(sql,val)
    result = cur.fetchone()
    return result[0]

def create_secu(cur,cusip_id, cp_id,fed_bk_date):
    sql = 'INSERT INTO "SECU" ("CUSIP_ID","COLT_POOL_ID","FED_BK_DATE") VALUES (%s,%s,%s) RETURNING "SECU_ID"'
    val = (cusip_id,cp_id,fed_bk_date)
    cur.execute(sql,val)
    result = cur.fetchone()
    return result[0]

def create_secu_stat(cur,secu_id, stat_cd, stat_dttm):
    sql = 'INSERT INTO "SECU_STAT" ("SECU_ID","SECU_STAT_CD","SECU_STAT_DTTM") VALUES (%s,%s,%s)'
    val = (secu_id,stat_cd,stat_dttm)
    cur.execute(sql,val)

def create_loan(cur,fnm_ln_no):
    sql = 'INSERT INTO "LN" ("FNM_LN_NO") VALUES (%s) RETURNING "LN_ID"'
    val = (fnm_ln_no,)
    cur.execute(sql,val)
    result = cur.fetchone()
    return result[0]

def create_loan_aqsn(cur,loan_id,upb,cvrg_cd,prdc_cd):
    sql = 'INSERT INTO "LN_AQSN" ("LN_ID","UPB","CVRG_CD","DUS_PRDC_CD") VALUES (%s,%s,%s,%s)'
    val=(loan_id,upb,cvrg_cd,prdc_cd)
    cur.execute(sql,val)

def create_mbs_pool_ln(cur,cp_id, loan_id):
    sql = 'INSERT INTO "MBS_POOL_LN" ("COLT_POOL_ID", "LN_ID") VALUES (%s,%s)'
    val= (cp_id,loan_id)
    cur.execute(sql,val)

try:
    conn = connect()
    cur = conn.cursor()

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)

securities = range(30)
for security in securities:
    mbs_pool_id = str(random_string())
    cusip_id = str(random_string())
    fed_bk_date= '2020-03-25'
    loans = range(8)
    cp_id= create_colt_pool(cur, mbs_pool_id)
    print(cp_id)
    secu_id= create_secu(cur,cusip_id,cp_id,fed_bk_date)
    create_secu_stat(cur,secu_id,'ACTV',datetime.datetime(2020,3,10))


    for loan in loans:
        fnm_ln_no= random_string(2) + str(random_int())
        upb=500000 + random.randint(5000, 100000)
        if str(upb)[-1]  != '5':
            prdc_cd='DUS'
        else:
            prdc_cd='BD'
        loan_id = create_loan(cur,fnm_ln_no)
        create_loan_aqsn(cur,loan_id,upb,'CVRD',prdc_cd)
        create_mbs_pool_ln(cur,cp_id, loan_id)

if(conn):
    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")

