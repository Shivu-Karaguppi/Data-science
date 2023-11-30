from databricks import sql
import pymssql
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import threading
user='tableau_ro'
password='ELN0pMjVe0kl'



def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0914-072437-cwq0vku"#qa
    # access_token="dapi3a452c566de3b6e1a86ae99d20945c5e"#qa 
    access_token="dapid0b0098b16f524093fa7a4c388de9f30"
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection


data_dict2 = {
    'col_name': ['None', 'None', 'None', 'None']    
}
server='Analyticsdwh01'
DB_names='Epos'

def src():
    ssms_cnxn=pymssql.connect(server, user, password, DB_names)
    src_query_mod="select * from INFORMATION_SCHEMA.TABLES where TABLE_CATALOG='Epos'  "
    try:
        global a,num_rows
        a=pd.read_sql(src_query_mod,ssms_cnxn)
        print(a)
        num_rows = len(a)
    except:
        print("src didn't work..")
src()
enrty={}
global tableq
global col_validn
# global tgt_cntq
tableq=[]
col_validn=[]
# tgt_cntq=[]
def src_vs_tgt(x):
    try: 
        DBconn=databricks()
        ssms_cnxn=pymssql.connect(server, user, password, DB_names)
        # for x in range(len(a)):
        table=a.loc[x,'TABLE_NAME'].lower()
        try:
            tgt_qry=f"""describe adp_core.dl11_bkp.{table} """
            tgt_df=pd.read_sql(tgt_qry,DBconn)
        except:
            tgt_df = pd.DataFrame(data_dict2)

        src_qry=f"""exec sp_columns [{table}] """

        src_df=pd.read_sql((src_qry),ssms_cnxn)

        tgt_col_name=tgt_df['col_name']
        src_col_names=src_df['COLUMN_NAME']
        print(f"""{x}>>{table}""")
        # print(src_col_names)
        # print(tgt_col_name)
        are_equal =src_col_names.equals(tgt_col_name)
        # if are_equal:
        #     print('The DataFrames are equal.')
            
        # else:
        #     print('The DataFrames are not equal.')
        tableq.append(table)
        col_validn.append(are_equal)
        print(are_equal)
        # tgt_cntq.append(tgt_cnt)
    except:
        pass


# src_vs_tgt()

def threadingz():
    # sr()
    threads = []
    for i in range(num_rows):
        thread = threading.Thread(target=src_vs_tgt, args=(i,))
        threads.append(thread)
        # time.sleep(0.5)
        thread.start()
    # Waits for all threads to finish
    for thread in threads:
        thread.join()
    print("All threads have finished")
threadingz()

def file_maker():
    entry = {'table': tableq, 'col_validn': col_validn}#, 'tgt_cnt': tgt_cntq}
    excel_file_path=r"""E:\ShivanandK\hd_pgr_struct_validation.xlsx"""
    final_table=pd.DataFrame(entry)
    final_table.to_excel(excel_file_path, index=False)
    print("completed..")
file_maker()