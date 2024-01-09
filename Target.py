from databricks import sql
import pyodbc
import os
import pandas as pd
import warnings
from datetime import datetime

result1 = pd.DataFrame()
result_mst_coxn=[]
# outputs_of_mst=pd.DataFrame()
dfr=pd.DataFrame()
customers=[2,14,13,9]
cust_num=[]
tab_name=[]
src_result=[]
target_result=[]
# odbc_driver='ODBC Driver 17 for SQL Server'
# df_config=pd.read_excel('config_o̥ds.xlsx',sheet_name='object_config',usecols=['target_table','target_dt_created','target_dt_updated'])



def databricks():
   host="bolt-analyticsprod.cloud.databricks.com"
   http_path="sql/protocolv1/o/3158924937735331/0425-072240-ijw0rg8h"
   access_token="dapicdda059cc93ed957a0f4670e651e12c4"#abdul_access_token

   connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
   return connection

def mst_coxn():
  connection=databricks()
  cursor=connection.cursor()
  df_objName=pd.read_sql('select distinct(obj_name) from adp_core.mst.mst_ods_customer_object_config',connection)
  # print(df)


  for y in range(len(df_objName)):
      # print(len(df))
      ODS_tables=df_objName.loc[y,'obj_name']
      # print(y)
      for cust in customers:
          # print(y,cust) #to_check_index&customer_number(49)
          cursor.execute(f"select conxn,DB,cust,t,tdc,tdu from (SELECT substring(connection_value, 0, (charindex(';', connection_value)-1)) conxn,substring(connection_value,(charindex(';', connection_value)+1)) DB,customer_id cust,target_table t,target_dt_created tdc,target_dt_updated tdu from adp_core.mst.mst_ods_customer_object_config WHERE obj_name ='{ODS_tables}' AND  customer_id ={cust} AND connection_value <> 'None')")
          # result2=pd.DataFrame(cursor.fetchall())
          data=cursor.fetchall()
          # print(data)
          result_mst_coxn.append(data)
          cols=list(map(lambda x:x[0],cursor.description))
          result2=(pd.DataFrame(data,columns=cols))
          # print(result_mst_coxn)
      print(df_objName.loc[y,'obj_name']) #to_check_table_names
      if ODS_tables=='ODS_ROLES_WF':
        break
         
  data_convert= [row[0] for row in result_mst_coxn]  
  outputs_of_mst = pd.DataFrame(data_convert,columns=['Conn_Val','DB_Name','cust_num','table','tdc','tdu'])
#   print(outputs_of_mst)
  return outputs_of_mst


warnings.filterwarnings('ignore')

def target():
   connection=databricks()
   

   dfx=mst_coxn()
   # print(len(dfx))
   for x in range(len(dfx)):
     table_name=dfx.loc[x,'table']
    #  #for z in range(len(customers)):
     cust_id=dfx.loc[x,'cust_num']
     try:
        cursor=connection.cursor()
        cursor.execute(f"select count(1) a from adp_core.dl{cust_id}.{table_name} where active_flag='YES'")
# select count(1) as cnt from policy where datecreated <= '2023-03-05 21:16:42.327' or dateupdated <= '2023-03-05 21:21:27.483' or  datecreated is null or dateupdated  is null"
        resul=cursor.fetchall()
        print(resul[0])

        target_result.append(resul)
        target_result[x].append(table_name)

        target_result[x].append(cust_id)
     except Exception as e:
              print('not_found')#throws type of exception

def if_else():
    dfx=mst_coxn()
    print(dfx.query('tdc is None'))
if_else()