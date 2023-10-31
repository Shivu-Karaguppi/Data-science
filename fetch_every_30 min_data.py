import sched
import time
import warnings
import pymssql
import pandas as pd 
from databricks import sql
import datetime


# print(curr)
warnings.filterwarnings('ignore')

user='analytics-prod-etl'
password='nUncog07eNfeLYAW'
tbl_nm='resultattachment'
customer_id=9
file_name =  r"E:\ShivanandK\saturday.txt"


def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0914-072437-cwq0vku"#qa
    access_token="dapi3a452c566de3b6e1a86ae99d20945c5e"#qa 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
     
    return connection

def src_val(src_tbl_nm,server,db):
    ssms_coxn=pymssql.connect(server, user, password, db)
    src_qry=f"select count(*) as src_cnt from {src_tbl_nm}"
    # dt_qry=f"select min(activity_date), max(activity_date) from  {src_tbl_nm}"
    # dt_qry=f"""select count(*) as src_cnt from {src_tbl_nm} --where activity_date<=cast("2000-01-01" as datetime) """
     
    query1 = f"""select groupid,city from [{src_tbl_nm}]-- where activity_date <= cast(getdate()-1 as datetime) or activity_date >=  cast ('1900-01-01' as datetime) or activity_date is null  """

    # qry=f"""select autoid from [{src_tbl_nm}] where dtquotelastupdated <= cast(getdate()-1 as datetime) and dtquotelastupdated >=  cast ('1900-01-01' as datetime) or dtquotelastupdated is null  """
    # qry=f"""select id  from [{src_tbl_nm}] where datecreated <= cast(getdate()-1 as datetime) or datecreated >=  cast ('1900-01-01' as datetime) or datecreated is null and (dateupdated<= cast(getdate()-1 as datetime) or dateupdated<= cast(getdate()-1 as datetime))  or dateupdated is null  or dateupdated is null """
    # src_df=pd.read_sql(query1,ssms_coxn)
    df1=pd.read_sql(src_qry,ssms_coxn)
    print(df1)
    return df1

def tgt_val(cust_id,tbl_nm):
    db_coxn=databricks()
    df1=pd.read_sql(f"select * from adp_core.test.config_table_qa cf left join adp_core.mst.mst_ods_customer_object_config ms on ms.customer_id=cf.customer_id and  cf.target_table=ms.target_table where cf.customer_id={cust_id} and cf.target_table='{tbl_nm}' ",db_coxn)
    print(df1)
    tgt_tbl_nm=df1.loc[0,'target_table']
    src_tbl_nm=tbl_nm#df1.loc[0,'source_table']
    server=df1.loc[0,'conxn']
    db=df1.loc[0,'database_nm']
    # tgt_dt_updated=df1.loc[0,'target_dt_updated']
    # tgt_dt_created=df1.loc[0,'target_dt_created']
    tgt_qry=f"select   count(*) tgt_cnt from adp_core.dl{cust_id}.{tbl_nm} where active_flag='YES' "
     
    tgt_df=pd.read_sql(tgt_qry,db_coxn)
    
    print(tgt_df)
    df1=src_val(src_tbl_nm,server,db)
    df2=tgt_df
    #  
    # result = df1.merge(df2, on=['groupid'], how='inner', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    result=pd.concat([df1,df2],axis=1)
    # print()
    print(result)
    with open(file_name, "a") as file:
    # Write the data to the file
        sd=str(result.loc[0,'src_cnt'])
        t_c=str(result.loc[0,'tgt_cnt'])
        print(sd)
        print(t_c)
        curr=str(datetime.datetime.now())
        file.write("src_cnt>"+sd+ " tgt_cnt>"+t_c + "  "+curr+"\n")
    

# tgt_val(customer_id,tbl_nm)

s = sched.scheduler(time.time, time.sleep)
interval = 20*60 

while True:
    s.enter(interval, 1, tgt_val, (customer_id,tbl_nm))
    s.run()
