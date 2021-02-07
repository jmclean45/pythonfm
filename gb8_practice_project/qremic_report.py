from common_util import *

def get_qremic_ids(cur):
    idsl= []
    cur.execute(""" SELECT "AGMT_NME" from "ST_TRAN_DL" WHERE "ST_TRAN_DL_CD" = 'Q-REMIC' ORDER BY "AGMT_NME" """)
    ids= cur.fetchall()
    for id in ids:
        idsl.append(id[0])
    return idsl

def get_iremic_ids(cur,qremic_id):
    idsl= []
    cur.execute(""" 
    SELECT distinct "AGMT_NME" from "ST_TRAN_DL" std 
    join "ST_TRAN_COLT_GRP" STC on STC."AGMT_ID" = std."AGMT_ID" 
    JOIN "FINS_GRP_ASSC" F on STC."ST_TRAN_CG_ID"= F."ST_TRAN_CG_ID"
    JOIN "SECU" S on F."SECU_ID" = S."SECU_ID"
	where S."SECU_ID" in ( select FG."SECU_ID" from "FINS_GRP_ASSC" FG
	join "ST_TRAN_COLT_GRP" stcg on FG."ST_TRAN_CG_ID" =stcg."ST_TRAN_CG_ID" 
	join "ST_TRAN_DL" std2 on std2."AGMT_ID" = stcg."AGMT_ID"
	where std2."AGMT_NME" = %s)
	and std."ST_TRAN_DL_CD" ='I-REMIC'
     """,(qremic_id,))
    ids= cur.fetchall()
    for id in ids:
        idsl.append(id[0])
    return idsl

def get_upb_sum(cur,iremic_id, reporting_pd):
    cur.execute(""" 
    select sum(lsa."UPB") as "LSA_UPB_SUM", sum(laq."UPB") as "AQSN_UPB_SUM" from "LN_SVCY_ACVY" lsa 
    join "LN_ACVY" la on lsa."LN_ACVY_ID" = la."LN_ACVY_ID" 
    join "LN_AQSN" laq on laq."LN_ID" = la."LN_ID"
    join "MBS_POOL_LN" mpl on la."LN_ID" = mpl."LN_ID" 
    join "SECU" s on mpl."COLT_POOL_ID" = s."COLT_POOL_ID" 
    join "FINS_GRP_ASSC" fga on s."SECU_ID" =fga."SECU_ID" 
    join "ST_TRAN_COLT_GRP" stcg on fga."ST_TRAN_CG_ID" = stcg."ST_TRAN_CG_ID" 
    join "ST_TRAN_DL" std on stcg."AGMT_ID" = std."AGMT_ID" 
    where std."AGMT_NME" = %s
    and lsa."RPTG_PD" = %s
    having count(la."LN_ID") = count(distinct laq."LN_ID") 
         """, (iremic_id, reporting_pd))
    sums = cur.fetchone()
    if sums is not None:
        return {"LSA_SUM":float(sums[0]),"AQSN_SUM":float(sums[1])}
    else:
        cur.execute("""
    select sum(lsa."UPB") as "LSA_UPB_SUM", sum(laq."UPB") as "AQSN_UPB_SUM" from "LN_SVCY_ACVY" lsa 
    join "LN_ACVY" la on lsa."LN_ACVY_ID" = la."LN_ACVY_ID" 
    join "LN_AQSN" laq on laq."LN_ID" = la."LN_ID"
    join "MBS_POOL_LN" mpl on la."LN_ID" = mpl."LN_ID" 
    join "SECU" s on mpl."COLT_POOL_ID" = s."COLT_POOL_ID" 
    join "FINS_GRP_ASSC" fga on s."SECU_ID" =fga."SECU_ID" 
    join "ST_TRAN_COLT_GRP" stcg on fga."ST_TRAN_CG_ID" = stcg."ST_TRAN_CG_ID" 
    join "ST_TRAN_DL" std on stcg."AGMT_ID" = std."AGMT_ID" 
    where std."AGMT_NME" = %s
    and la."LN_ACVY_DTTM" = (select max("LN_ACVY_DTTM") from "LN_ACVY" where "LN_ID" = la."LN_ID")
        """,(iremic_id))
        cur.fetchone()
        return {"LSA_SUM": float(sums[0]), "AQSN_SUM": float(sums[1])}


def lambda_handler():
    conn = connect()
    cur = conn.cursor()
    month = '02'
    year = '2020'
    reporting_pd= year+month
    qremic_ids=get_qremic_ids(cur)
    if month in ('03','06','09','12'):
        qremic_creation_month= True
        print("QMONTH")
    else:
        qremic_creation_month = False
    for qremic_id in qremic_ids:
        iremic_ids= get_iremic_ids(cur,qremic_id)
        for iremic_id in iremic_ids:
            if qremic_creation_month == True and iremic_id in iremic_ids[-3:]:
                sum=get_upb_sum(cur,iremic_id,reporting_pd)['AQSN_SUM']
            else:
                sum=get_upb_sum(cur,iremic_id,reporting_pd)['LSA_SUM']
            print(iremic_id,sum)

    if(conn):
        conn.commit()
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")

lambda_handler()