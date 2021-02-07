from common_util import *

def get_loans(cur):
    loans = []
    cur.execute('SELECT "LN_ID","UPB" from "LN_AQSN"')
    result = cur.fetchall()
    for r in result:
        loans.append({'LN_ID':r[0],'UPB':float(r[1])})
    return loans

def create_ln_acvy(cur,loan_id,dttm,rptg_pd,upb):
    sql = 'INSERT INTO "LN_ACVY" ("LN_ID","LN_ACVY_DTTM") VALUES (%s,%s) RETURNING "LN_ACVY_ID"'
    val = (loan_id,dttm)
    cur.execute(sql, val)
    result = cur.fetchone()
    ln_acvy_id=result[0]
    sql2='INSERT INTO "LN_SVCY_ACVY" ("LN_ACVY_ID","UPB","RPTG_PD") VALUES (%s,%s,%s)'
    val2=(ln_acvy_id,upb,rptg_pd)
    cur.execute(sql2,val2)

def lambda_handler():
    conn = connect()
    cur = conn.cursor()

    loans = get_loans(cur)

    dttm='2020-03-22'
    rptg_pd= 202002
    for loan in loans:
        upb = float(loan['UPB']) - random.randint(1000,10000)
        create_ln_acvy(cur,loan['LN_ID'],dttm,rptg_pd, upb)

    if(conn):
        conn.commit()
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")

lambda_handler()