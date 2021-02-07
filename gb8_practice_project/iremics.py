
import psycopg2
from common_util import *

def get_valid_ids(cur, start,end):
    sql= """
    SELECT "SECU_ID" FROM "SECU" WHERE "FED_BK_DATE" BETWEEN %s AND %s
    """
    val= (start, end)
    cur.execute(sql,val)
    valids= cur.fetchall()
    return valids

def create_agmt(cur,cd):
    sql='INSERT INTO "AGMT" ("AGMT_CD") VALUES (%s) RETURNING "AGMT_ID"'
    val= (cd,)
    cur.execute(sql,val)
    agmt_id=cur.fetchone()
    return agmt_id[0]

def create_std(cur,agmt_id, iremic_id):
    sql="""INSERT INTO "ST_TRAN_DL" ("AGMT_ID","AGMT_NME","ST_TRAN_DL_CD") VALUES (%s,%s,'I-REMIC')"""
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

def get_remic_id(cur):
    cur.execute(""" select max("AGMT_NME") FROM "ST_TRAN_DL" WHERE "ST_TRAN_DL_CD'='I-REMIC' """)
    id = cur.fetchone()
    return id[0]

try:
    conn = psycopg2.connect(user = "postgres", password = "near476", host = "localhost", port = "5432", database = "Practice")
    cur = conn.cursor()


except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)


year = '2020'
#select the month before the month that it is run in
month= '02'


month_start, month_end = get_1st_and_lst_day(int(year),int(month))

final_dates = get_final_dates(int(month),int(year))

for remic_range in final_dates:
    prv_id= get_remic_id(cur)
    if prv_id is not None:
        iremic_id = "M020I"+ str(int(prv_id[-2:])+1)
    else:
        iremic_id= "M020I10"
    print(iremic_id)
    agmt_id= create_agmt(cur, 0)
    create_std(cur,agmt_id, iremic_id)
    create_stds(cur, agmt_id, 'OPND', remic_range[0])
    create_stds(cur, agmt_id, 'STLD', remic_range[1])
    st_colt_grp_id= create_stcg(cur, agmt_id)
    valid_secus= get_valid_ids(cur, remic_range[0], remic_range[1])
    if bool(valid_secus):
        for valid_sec in valid_secus:
            create_fins_assc(cur, st_colt_grp_id,valid_sec[0])

if(conn):
    conn.commit()
    cur.close()
    conn.close()
    print("PostgreSQL connection is closed")