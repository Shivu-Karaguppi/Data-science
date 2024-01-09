from databricks import sql
import pyodbc
import os
import pandas as pd
import warnings
from datetime import datetime
import warnings
from datetime import datetime as dt

warnings.filterwarnings("ignore")

result1 = pd.DataFrame()
result_mst_coxn=[]
# outputs_of_mst=pd.DataFrame()
# df3=pd.DataFrame()
dfz=[]
customers=[2,14,13,9,11,24,17,22,30,26,38,36]
cust_num=[]
tab_name=[]
src_result=[]
target_result=[]


print("started..")
def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0525-070435-t5v0j694"#my_cluster
    access_token="dapid0901d27ea66595762a8a5484812b458"#my_access_token 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection
# databricks()

def mst_coxn():
    connection=databricks()
    cursor=connection.cursor()
    df_objName=pd.read_sql("""select distinct(target_table) obj_name from adp_core.mst.mst_ods_customer_object_config""",connection)
    dfz=[]
    # for y in range(1):
    for y in range(len(df_objName)):
        for cust in customers:
            ODS_tables=df_objName.loc[y,'obj_name']
            # customers
            print(y)
            try:
                df1=pd.read_sql(f"""select max as max_vesrsion,max_version_time from (select version as max,  date_format(timestamp,"yyyy-MM-dd HH:mm:ss.SSS") as max_version_time from (describe history adp_core.dl{cust}.{ODS_tables}) limit 1)""",connection)
                # print(df1)
                df2=pd.read_sql(f"""select source_table,b.obj_name ,a.src_ctrl1,a.trgt_ctrl1,aud_batch_id,b.customer_id,b.target_table,substring(connection_value, 0, (charindex(';', connection_value)-1)) conxn,substring(connection_value,(charindex(';', connection_value)+1)) DB,target_dt_created,target_dt_updated,b.source_pkey,b.target_pkey from adp_core.admin.aud_table_log_airflow a join adp_core.MST.mst_ods_customer_object_config b on a.obj_id = b.obj_id  WHERE target_table='{ODS_tables}' and b.customer_id={cust} """,connection)
                # print(df2) 
            except:   
                  continue
            df3=pd.concat([df1, df2], axis=1).fillna("0")
            dfz.append(df3)
    dfc = pd.concat(dfz, ignore_index=True)
    print(dfc)
    curr=datetime.now()
    curr_time=curr.date()
    file_path=f"""C:\\Users\\shivanandk\\BOLT Solutions\Analytics - Documents\\New DWH\\QA\\max_version_dl ({curr_time}).xlsx"""
    dfc.to_excel(file_path)
    print("Completed..")
    # return outputs_of_mst
mst_coxn()