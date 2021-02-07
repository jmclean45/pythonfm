import collections
from common_util import *
import psycopg2

def get_remic_id(cur):
    cur.execute(""" SELECT MAX("AGMT_NME") from "ST_TRAN_DL" WHERE "ST_TRAN_DL_CD" = 'Q-REMIC' """)
    id = cur.fetchone()
    return id[0]

def get_time_struct_time():
    time= collections.namedtuple('time', 'tm_year, tm_mon tm_mday')
    year = '2020'
    month = '03'
    day = '01'
    time_struct_time = time(tm_year=int(year),tm_mon= int(month), tm_mday=int(day))
    return time_struct_time

# def  query_stds_by_opnd(cur, start, end):
#     sql= """ SELECT STDS."AGMT_ID", STDS."ST_TRAN_DL_ST_DTTM" FROM "ST_TRAN_DL_ST" STDS, "ST_TRAN_DL" S
#      WHERE "ST_TRAN_DL_ST_DTTM" BETWEEN '2020-04-30' and '2020-08-01' AND "ST_TRAN_DL_ST_CD" ='OPND' AND S."AGMT_NME" not like '%Q%'
#      AND S."AGMT_ID" = STDS."AGMT_ID" """
#     cur.execute(sql)
#     vals=cur.fetchall()
#     print(vals)
#     return vals
    # sql= """ SELECT STDS."AGMT_ID", STDS."ST_TRAN_DL_ST_DTTM" FROM "ST_TRAN_DL_ST" STDS, "ST_TRAN_DL" S
    # WHERE "ST_TRAN_DL_ST_DTTM" BETWEEN %s and %s AND "ST_TRAN_DL_ST_CD" ='OPND' AND S."AGMT_NME" not like '%Q%'
    # AND S."AGMT_ID" = STDS."AGMT_ID" """
    # val = (start,end)
    # cur.execute(sql, val)
    # first_iremics= cur.fetchall()
    # return first_iremics

def query_for_ids():
    id_list =[]
    sql = """
    SELECT DISTINCT "AGMT_ID" FROM "ST_TRAN_COLT_GRP" STC
    JOIN "FINS_GRP_ASSC" F on STC."ST_TRAN_CG_ID"= F."ST_TRAN_CG_ID"
    JOIN "SECU" S on F."SECU_ID" = S."SECU_ID"
    WHERE EXTRACT(MONTH FROM "FED_BK_DATE") between '01' and '02'
    AND EXTRACT(YEAR FROM "FED_BK_DATE") = '2020'
    """
    cur.execute(sql)
    ids=cur.fetchall()
    for id in ids:
        id_list.append(id[0])
    return id_list

def create_agmt(cur,cd):
    sql='INSERT INTO "AGMT" ("AGMT_CD") VALUES (%s) RETURNING "AGMT_ID"'
    val= (cd,)
    cur.execute(sql, val)
    agmt_id=cur.fetchone()
    return agmt_id[0]

def create_std(cur,agmt_id, iremic_id):
    sql="""INSERT INTO "ST_TRAN_DL" ("AGMT_ID","AGMT_NME","ST_TRAN_DL_CD") VALUES (%s,%s,'Q-REMIC')"""
    val= (agmt_id, iremic_id)
    cur.execute(sql,val)

def create_stds(cur, agmt_id, st_cd, dttm):
    sql = """INSERT INTO "ST_TRAN_DL_ST" ("AGMT_ID","ST_TRAN_DL_ST_CD","ST_TRAN_DL_ST_DTTM") VALUES (%s,%s,%s)"""
    val = (agmt_id, st_cd, dttm)
    cur.execute(sql, val)

def create_stcg(cur, agmt_id):
    sql= """INSERT INTO "ST_TRAN_COLT_GRP"("AGMT_ID") VALUES (%s) RETURNING "ST_TRAN_CG_ID" """
    val = (agmt_id,)
    cur.execute(sql,val)
    id= cur.fetchone()
    return id[0]

def create_fins_assc(cur, st_colt_grp_id, secu_id):
    sql= """INSERT INTO "FINS_GRP_ASSC" ("ST_TRAN_CG_ID", "SECU_ID") VALUES (%s,%s)"""
    val= (st_colt_grp_id,secu_id)
    cur.execute(sql,val)

def query_fins(cur, agmt_ids):
    sql= """SELECT "SECU_ID" FROM "FINS_GRP_ASSC" WHERE "ST_TRAN_CG_ID" IN (
    SELECT "ST_TRAN_CG_ID" FROM "ST_TRAN_COLT_GRP" WHERE "AGMT_ID" IN %s) """
    val= (agmt_ids,)
    cur.execute(sql,val)
    secus= cur.fetchall()
    print(secus)
    return secus

def lambda_handler():
    try:
        conn = psycopg2.connect(user = "postgres", password = "near476", host = "localhost", port = "5432", database = "Practice")
        cur = conn.cursor()

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)

    now= get_time_struct_time()
    year = '2020'
    month1= '01'
    month2= '02'
    day = '01'

    iremic_agmt_ids= query_for_ids()
    qremic_secus = query_fins(cur, tuple(iremic_agmt_ids))
    qremic_start_and_end = ('2020-01-31','2020-02-09')

    prv_id = get_remic_id(cur)
    if prv_id is not None:
        qremic_id = "M020Q0" + str(int(prv_id[-1]) + 1)
    else:
        qremic_id = "M020Q01"
    agmt_id= create_agmt(cur, 0)
    create_std(cur,agmt_id, qremic_id)
    create_stds(cur, agmt_id, 'OPND', qremic_start_and_end[0])
    create_stds(cur, agmt_id, 'STLD', qremic_start_and_end[1])
    st_colt_grp_id= create_stcg(cur, agmt_id)
    for sec in qremic_secus:
        create_fins_assc(cur, st_colt_grp_id, sec[0])


    if(conn):
        conn.commit()
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
try:
    conn = psycopg2.connect(user="postgres", password="near476", host="localhost", port="5432", database="Practice")
    cur = conn.cursor()

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

lambda_handler()