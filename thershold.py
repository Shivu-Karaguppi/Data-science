from databricks import sql
import pandas as pd
import warnings
from pytz import timezone 
import pyodbc 
from datetime import datetime,timedelta as tm
from utilities.databricks_connector import DatabricksConnector, DatabricksConnectorAuto
from airflow.models import Variable

ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
odbc_driver='ODBC Driver 17 for SQL Server'
given_date = datetime.now()
previous_day = given_date - tm(days=1)
formatted_date = previous_day.strftime("%Y-%m-%d")
print("Previous Day:", formatted_date )
thershold_dt=formatted_date+" 00:00:000"


class count_validation():
   
    def count_thershold(customer_id, path):
        DBconn = DatabricksConnectorAuto()
        mst_qry = [f"""select distinct lower(SUBSTR(obj_name, 5, length(obj_name)-7)) as table_name, customer_id,subject_area,
                   target_dt_created,target_dt_updated,substring(connection_value, 0, (charindex(';', connection_value)-1)) conxn,
                   substring(connection_value,(charindex(';', connection_value)+1)) DB
            from {path}.mst.mst_ods_customer_object_config where customer_id = {customer_id} and connection_value <> 'None' \
                and lower(SUBSTR(obj_name, 5, length(obj_name)-7)) in (select distinct table_name from system.information_schema.tables );""" ]
        mst_tbl_lst = DBconn.fetch(mst_qry)
        # qry = []
        print(mst_tbl_lst)
        if len(mst_tbl_lst) > 0:
            for tbl in mst_tbl_lst:
                tab_nm = tbl[0]
                subject_area=tbl[2]
                target_dt_created=tbl[3]
                target_dt_updated=tbl[4]
                conxn=tbl[5]
                database_nm=tbl[6]
                # conxn='10.131.0.137'
                if target_dt_created==None:
                    DBconn.execute_queries([f""" INSERT INTO {path}.test.count_validation  
                    (select '{customer_id}','{tbl}','{target_dt_createdx(tab_nm,conxn,database_nm)}',count(*),'{subject_area}','{ind_time}'
                    from adp_core.dl{customer_id}.{tbl} where active_flag='YES')"""])
                if target_dt_updated==None:
                   DBconn.execute_queries([f""" INSERT INTO {path}.test.count_validation  
                    (select '{customer_id}','{tbl}','{target_dt_updatedx(tab_nm,conxn,database_nm,target_dt_created,thershold_dt)}',count(*),'{subject_area}','{ind_time}'
                    from adp_core.dl{customer_id}.{tbl} where active_flag='YES')"""])
                else :
                   DBconn.execute_queries([f""" INSERT INTO {path}.test.count_validation  
                    (select '{customer_id}','{tbl}','{tgt_created_updated(tab_nm,conxn,database_nm,target_dt_created,target_dt_updated,thershold_dt)}',count(*),'{subject_area}','{ind_time}'
                    from adp_core.dl{customer_id}.{tbl} where active_flag='YES')"""])
                #    tgt_created_updated
                   
            # tbl_max_lst = DBconn.fetch(qry)
            # print("ODS Query Executed successfully", customer_id, tbl_max_lst)
        else:
            print("No Data found for " )



def target_dt_createdx(table_name,conxn,DB): # if datecreated column has null values
    # odbc_driver='ODBC Driver 17 for SQL Server'
    # server_names=conxn
    # DB_names=DB
    print(table_name,conxn,DB)
    cnxn = pyodbc.connect(f"""Driver={odbc_driver};Server={conxn};Database={DB};Trusted_Connection=yes""")
    cursor=cnxn.cursor()
    query=f"select count(1) Counts from [{table_name}]",
    cursor.execute(query)
    return cursor.fetchall()[0][0]

def target_dt_updatedx(table_name,conxn,DB,target_dt_created,business_end_dt):#if dateupdated column has null values
    # odbc_driver='ODBC Driver 17 for SQL Server'
    # server_names=conxn
    # DB_names=DB
    print(table_name,conxn,DB,target_dt_created,business_end_dt)
    cnxn = pyodbc.connect(f"""Driver={odbc_driver};Server={conxn};Database={DB};Trusted_Connection=yes""")
    query1 = f"""select count(*) from [{table_name}] where {target_dt_created} <='{business_end_dt}' or {target_dt_created} is null """
    query2=f"""select count(1) Counts from [{table_name}]"""
    query3=f"select '0.0' as Counts"# some tables dont have dateupdated column in src
    cursor=cnxn.cursor()
    try:
       cursor.execute(query1)
       print('query 1')
       return cursor.fetchall()[0][0] 
    except :
       cursor.execute(query2)
       print('query 2')
       return cursor.fetchall()[0][0]

def tgt_created_updated(table_name,conxn,DB,target_dt_created,target_dt_updated,business_end_dt):#done
    # odbc_driver='ODBC Driver 17 for SQL Server'
    # server_names=conxn
    # DB_names=DB
    print(table_name,conxn,DB,target_dt_created,business_end_dt)
    cnxn = pyodbc.connect(f"""Driver={odbc_driver};Server={conxn};Database={DB};Trusted_Connection=yes""")
    query1 = f"""select count(*) from [{table_name}] where {target_dt_created} <='{business_end_dt}' or {target_dt_updated}<='{business_end_dt}' or {target_dt_created} is null or {target_dt_updated} is null"""
    query2 = f"select count(1) as Counts from [{table_name}]"
    query3=f"select '0.0' as Counts"# some tables dont have datecreated column in tgt
    cursor=cnxn.cursor()
    try:
       cursor.execute(query1)
       print('query 1')
       return cursor.fetchall()[0][0] 
    except :
       cursor.execute(query2)
       print('query 2')
       return cursor.fetchall()[0][0]
