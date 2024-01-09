from databricks import sql
import pyodbc
import os
import pandas as pd
import warnings
from datetime import datetime,timedelta as tm
from datetime import datetime
from pytz import timezone 

warnings.filterwarnings("ignore")

result1 = pd.DataFrame()
result_mst_coxn=[]
# outputs_of_mst=pd.DataFrame()
dfr=pd.DataFrame()
src=[]
customers=[2,14,13,9,11,24,17,22,30,26,38,36]
given_date = datetime.now()
previous_day = given_date - tm(days=1)
formatted_date = previous_day.strftime("%Y-%m-%d")
thershold_dt=formatted_date+" 00:00:000"
data = {
    'count': [],
    'name': [],
    'customer_number': []
}
odbc_driver='ODBC Driver 17 for SQL Server'
print("started...")

ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')


def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0914-072437-cwq0vku"#qa
    access_token="dapi3a452c566de3b6e1a86ae99d20945c5e"#qa 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection
# databricks()

# conx=databricks()
# cur=conx.cursor()
# cur.execute("select count(*) from adp_core.dl2.policy ")
# fetch=cur.fetchall()
# print(fetch)

def dwh_max_version(customer_id, path):
        DBconn=databricks()
        cursor=DBconn.cursor()
        mst_qry = f"select distinct lower(SUBSTR(obj_name, 5, length(obj_name)-7)) as table_name, customer_id \
            from {path}.mst.dwh_layer_customer_object_config \
            where customer_id = {customer_id} and obj_name like 'DWH_%' and lower(SUBSTR(obj_name, 5, length(obj_name)-7)) \
            in (select table_name from system.information_schema.tables );" 
        cursor.execute(mst_qry)
        mst_tbl_lst=cursor.fetchall()
        print(mst_tbl_lst)
        qry = []
        # tblz=[]
        # for tbl in mst_tbl_lst:
        #      tblz.append(tbl)
        if len(mst_tbl_lst) > 0:
            for tbl in mst_tbl_lst:
                print(tbl)
                table_name=tbl.table_name
                try:
                    qry=(f""" WITH TempTable AS ( \
                    select {customer_id} as customer_id , '{table_name}' as table_name, max(version) as max_version, '{ind_time}' as fetched_on ,timestamp as max_timestamp\
                    from (describe history {path}.dwh.{table_name}) group by timestamp order by max(version) desc limit 1  ) 
                     INSERT INTO {path}.test.dwh_max_version SELECT * FROM TempTable; """)
                
                # print("sksk")
                    cursor.execute(qry)
                except Exception as e:
                    print(e)
                # print("DWH Query Executed successfully", customer_id, tbl_max_lst)
        else:
            print("No Data found")

for cust in customers :
    dwh_max_version(cust,"adp_core")



