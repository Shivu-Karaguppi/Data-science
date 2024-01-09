import os
import glob
from databricks import sql
import pandas as pd
from datetime import datetime 
import warnings
warnings.filterwarnings("ignore")


# folder_path = r'C:\Users\shivanandk\BOLT Solutions\Analytics - Documents\New DWH\ETL Specs\DWH To Denormalized Layer'
# dn_tbl_lst = os.listdir(folder_path)
# print(dn_tbl_lst)

dn_tbl_lst=['DN_AGENT.xlsx', 'DN_APPLICATION.xlsx', 'DN_CALL_CENTER.xlsx', 'DN_CALL_SURVEY.xlsx', 'DN_CARRIER_QUOTE_DETAILS.xlsx', 'DN_CARRIER_QUOTE_MESSAGES.xlsx', 'DN_CONSUMER.xlsx', 'DN_HIERARCHY.xlsx', 'DN_POLICY.xlsx', 'DN_PREFILL_DETAILS.xlsx', 'DN_SALES_FUNNEL.xlsx', 'DN_WORKFLOW.xlsx']

def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0525-070435-t5v0j694"#my_cluster
    access_token="dapid0901d27ea66595762a8a5484812b458"#my_access_token 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection

def max_dn():
  df1=pd.DataFrame(columns=["max_dn_ver","table_name"])
  for table in dn_tbl_lst:
       try:
            df=pd.read_sql(f"select max(version) as max_version from (describe history adp_core.dn.{table[:-5]} )",databricks())
            val=df['max_version'][0]
            print(val)
            tab=table[:-5]
            df1=df1.append({'max_dn_ver':val,'table_name':tab}, ignore_index=True)
       except:
            pass
  print(df1)
  curr=datetime.now()
  curr_time=curr.date()
  file_path=f"""C:\\Users\\shivanandk\\BOLT Solutions\Analytics - Documents\\New DWH\\QA\\max_version_dn ({curr_time}).xlsx"""
  df1.to_excel(file_path)
  print("Completed..")
max_dn()

