import psycopg2

def get_loan_data():
    sql="""
    SELECT 
    L."LN_ID",L."FNM_LN_NO",LA."UPB", MPL."COLT_POOL_ID", CP."MBS_POOL_ID", S."SECU_ID",
    S."FED_BK_DATE", S."CUSIP_ID", SS."SECU_STAT_CD", SS."SECU_STAT_DTTM", CTR."CVRG_TYP", PTR."PRDC_TYP",
    AG."AGMT_ID", STD."ST_TRAN_DL_CD", STD."AGMT_NME", STDS."ST_TRAN_DL_ST_CD"
    
    FROM 
    "LN" L join "LN_AQSN" LA on L."LN_ID"=LA."LN_ID"
    join "MBS_POOL_LN" MPL on LA."LN_ID"=MPL."LN_ID"
    join "COLT_POOL" CP on MPL."COLT_POOL_ID" = CP."COLT_POOL_ID"
    join "SECU" S on CP."COLT_POOL_ID" = S."COLT_POOL_ID"
    join "SECU_STAT" SS on S."SECU_ID"= SS."SECU_ID"
    join "LN_CVRG_TYP_REF" CTR on LA."CVRG_CD"= CTR."CVRG_CD"
    join "LN_PRDC_TYP_REF" PTR on LA."DUS_PRDC_CD"= PTR."PRDC_CD"
    join "FINS_GRP_ASSC" F on S."SECU_ID" = F."SECU_ID"
    join "ST_TRAN_COLT_GRP" STCG on F."ST_TRAN_CG_ID" =  STCG."ST_TRAN_CG_ID"
    join "AGMT" AG on STCG."AGMT_ID" = AG."AGMT_ID"
    join "ST_TRAN_DL" STD on  AG."AGMT_ID"= STD."AGMT_ID"
    join "ST_TRAN_DL_ST" STDS on AG."AGMT_ID" = STDS."AGMT_ID" 
    
    WHERE
    EXTRACT(Month from S."FED_BK_DATE") = %s
    and EXTRACT(Year from S."FED_BK_DATE") =%s
    and SS."SECU_STAT_CD" = 'ACTV'
    """

def get_qremic_start_and_end(cur,qremic_id):
    sql= """
    SELECT STDS."ST_TRAN_DL_ST_DTTM" FROM "ST_TRAN_DL_ST" STDS, "ST_TRAN_DL" STD 
    WHERE STDS."ST_TRAN_DL_ST_CD" = %s AND STD."AGMT_NME" = %s AND STDS."AGMT_ID"= STD."AGMT_ID"
    """
    opnd_val= ('OPND',qremic_id)
    stld_val= ('STLD',qremic_id)
    cur.execute(sql,opnd_val)
    start_date=cur.fetchall()
    cur.execute(sql,stld_val)
    end_date = cur.fetchone()
    print(start_date[0])
    print(start_date)
    print(end_date)

try:
    conn = psycopg2.connect(user="postgres", password="near476", host="localhost", port="5432", database="Practice")
    cur = conn.cursor()

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

get_qremic_start_and_end(cur,'M0Q393')
