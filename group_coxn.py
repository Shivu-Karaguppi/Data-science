import threading
import time
from databricks import sql
import pandas as pd
import warnings
from pytz import timezone 
# import pyodbc 
import pymssql
from datetime import datetime,timedelta as tm
from airflow.models import Variable
import warnings
from pytz import timezone 
import pymssql

warnings.filterwarnings('ignore')
ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
odbc_driver='ODBC Driver 17 for SQL Server'
given_date = datetime.now()
previous_day = given_date - tm(days=1)
formatted_date = previous_day.strftime("%Y-%m-%d")
print("Previous Day:", formatted_date )
business_end_dt=formatted_date+" 00:00:000"
business_start_dt='1900-01-01'
min_date='1900-01-01'

# def insert_sentence_in_file(filename, sentence, line_number):
#     try:
#         # Open the file in read mode to read its content
#         with open(filename, 'r') as file:
#             lines = file.readlines()

#         # Ensure the line_number is within the bounds
#         if line_number < 0:
#             line_number = 0
#         elif line_number > len(lines):
#             line_number = len(lines)

#         # Insert the sentence at the specified line_number
#         lines.insert(line_number, sentence + '\n\n')

#         # Open the file in write mode to write the modified content
#         with open(filename, 'w') as file:
#             file.writelines(lines)

#         print(f"Sentence inserted at line {line_number + 1} in {filename}.")
#     except FileNotFoundError:
#         print(f"File {filename} not found.")
#     except Exception as e:
#         print(f"An error occurred: {e}")

# filename = r"E:\ShivanandK\saturday.txt"
# line_to_insert_at = 2 


def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0914-072437-cwq0vku"#qa
    access_token="dapi3a452c566de3b6e1a86ae99d20945c5e"#qa 
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection

def modifier(queries):
        words=queries.split()
        words = words[:-2]
        new_query = ' '.join(words)
        return new_query

def target_dt_createdx(customer_id,table_name,conxn,DB,target_tbl): # if datecreated&dateupdated column has null values
    print(table_name,conxn,DB)
    query=f"select count(1) as cnt,'{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table  from [{table_name}] union all "
    return query


def target_dt_updatedx(customer_id,table_name,conxn,DB,target_dt_created,business_end_dt,min_date,target_tbl):#if dateupdated column has null values

    query1 = f"""select count(*) cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table from [{table_name}] where {target_dt_created} <= cast('{business_end_dt}' as datetime) or {target_dt_created} >=  cast ('{business_start_dt}' as datetime) or {target_dt_created} is null union all """
    return query1


def tgt_created_updated(customer_id,table_name,conxn,DB,target_dt_created,target_dt_updated,business_end_dt,min_date,target_tbl):#done

    query1 = f"""select count(*) cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table from [{table_name}] where {target_dt_created} <= cast('{business_end_dt}' as datetime) or {target_dt_created} >=  cast ('{business_start_dt}' as datetime) 
    or {target_dt_created} is null or
    {target_dt_updated} <= cast('{business_end_dt}' as datetime) or {target_dt_updated} >=  cast ('{business_start_dt}' as datetime) or {target_dt_updated} is null union all """    
    return query1



tgt_full_query=""
src_dfs=[]

def tgt_query_executer():
    DBconn=databricks()
    try:
        tgt_df=pd.read_sql(tgt_full_query,DBconn)
    except :
        pass
    # return tgt_df
    
def dwh_max_version():
        tgt_full_query=""
        src_dfs=[]
        DBconn=databricks()
        cursor=DBconn.cursor()
        mst_qry = f"""select conxn,database_nm as db ,count(*) as ct from  (select  target_table as table_name, customer_id,subject_area,
                   target_dt_created,target_dt_updated, conxn,
                   database_nm,source_table,customer_id
                   from adp_core.test.config_table_qa ) 
                group by conxn,database_nm  """
        global df1
        df1=pd.read_sql(mst_qry,DBconn)
        global num_threads
        num_threads = len(df1)
        return df1


user='analytics-prod-etl'
password='nUncog07eNfeLYAW'

def grp_conxn(num_threads):
    src_dfs=[]
    tgt_full_query=""
    DBconn=databricks()
    cursor=DBconn.cursor()
    x=num_threads
    count=df1.loc[x,'ct']
    # print(count)
    cox=df1.loc[x,'conxn']
    db=df1.loc[x,'db']
    query2=f"""select * from adp_core.test.config_table_qa
            where conxn='{cox}' and database_nm='{db}' 
            --and target_table not in (select distinct target_table from (select case when target_table=source_table then 'pass' else 'fail' end as pass ,* from adp_core.test.config_table_qa) where pass='fail')"""
    dfz=pd.read_sql(query2,DBconn)
    src_query=""
    # print(cox,db)
    for x in range(count):
        customer_id = int(dfz.loc[x,'customer_id'])
        target_tbl= dfz.loc[x,'target_table']
        conxn=dfz.loc[x,'conxn']
        target_dt_created=dfz.loc[x,'target_dt_created']  
        target_dt_updated=dfz.loc[x,'target_dt_updated']
        DB=dfz.loc[x,'database_nm']
        src_tbl=dfz.loc[x,'source_table']
        sub_area=dfz.loc[x,'subject_area']
        tgt_query=f"select count(*) as cnt ,'{target_tbl}' as tbl ,'{customer_id}' as cust,'{sub_area}' as sub_area,'{ind_time}' as fetched_on ,'{src_tbl}' as src_tbl \n from adp_core.dl{customer_id}.{target_tbl} where active_flag='YES' union all "
        tgt_full_query+=tgt_query
        if target_dt_created=='0':  
            src_query+=target_dt_createdx(customer_id,src_tbl,conxn,DB,target_tbl)
        elif target_dt_updated=='0':
            src_query+=target_dt_updatedx(customer_id,src_tbl,conxn,DB,target_dt_created,business_end_dt,min_date,target_tbl)
        else :
            src_query+=tgt_created_updated(customer_id,src_tbl,conxn,DB,target_dt_created,target_dt_updated,business_end_dt,min_date,target_tbl)
    src_query_mod=modifier(src_query)
    server=conxn
    DB_names=DB
    ssms_cnxn=pymssql.connect(server, user, password, DB_names)
    try:
        a=f"df{x}"
        a=pd.read_sql(src_query_mod,ssms_cnxn)
        # print(a)
        src_dfs.append(a)
    except:
        print("except..src...")
        pass
    src_dfs=pd.concat(src_dfs, ignore_index=True,axis=0)
    df_tgt_final=pd.read_sql(modifier(tgt_full_query),DBconn)
    # print(src_dfs)
    # print(df_tgt_final)
    left_join=df_tgt_final.merge(src_dfs,how='outer', left_on=['tbl','cust'],right_on=['tgt_table','cust'],suffixes=['_tgt','_src'])
    print(left_join)

    for x in range(len(left_join)):
        c_src=left_join.loc[x,'cnt_tgt']
        t_tgt=left_join.loc[x,'cnt_src']
        cust=left_join.loc[x,'cust']
        tgt_table=left_join.loc[x,'tgt_table']
        fetched_on=left_join.loc[x,'fetched_on']
        src_table=left_join.loc[x,'tgt_table']
        ins_query=f"INSERT INTO adp_core.test.count_validation2 values ({cust},'{tgt_table}','{c_src}','{t_tgt}','{sub_area}','{src_table}','{fetched_on}');"
        # ins_queries+=ins_query
        cursor.execute(ins_query)
    print("completed...")



def threadingz():
    dwh_max_version()
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=grp_conxn, args=(i,))
        threads.append(thread)
        # time.sleep(3)
        thread.start()
    # Waits for all threads to finish
    for thread in threads:
        thread.join()
    print("All threads have finished")
threadingz()

