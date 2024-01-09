from databricks import sql
import pandas as pd
import warnings
import datetime 


local_time = str(datetime.datetime.now())[:-15]
print(local_time)
warnings.filterwarnings("ignore")

result1 = pd.DataFrame()


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
    df_objName=pd.read_sql("""select distinct obj_name  from adp_core.mst.dwh_layer_customer_object_config """,connection)

    dfz=[]
    listx=[]
    df3= pd.DataFrame()
    # dfw= pd.DataFrame()
    dfw=pd.DataFrame(columns=["max_dwh_ver","table_name"])
    for y in range(len(df_objName)):
        dfz.append(df_objName.loc[y,'obj_name'])
    for ele in dfz:
        try:
            dft=pd.read_sql(f"""select max(version) as max from (describe history adp_core.dwh.{ele[4:-3]}) """,connection)#['max'][0]
            # print(dft)
            table_name=ele[4:-3] 
            df3=pd.concat([df3,dft],ignore_index=True)
            dft=dft['max'][0]
            dfw= dfw.append({'max_dwh_ver': dft,'table_name':table_name}, ignore_index=True)
            listx.append(f"('{dft}','{table_name}','{local_time}',null,null,null,null,null)" )
        except Exception as e:
            pass
        # print(df3)
    values = ','.join(listx)
    q=[f"""INSERT INTO adp_core.test.dwh_max_version VALUES {values} """]
    pd.read_sql(q,connection)
    print(values)
    # print(df3)
    print(dfw)
    # table_name=f'max_version_{local_time}'
    # file_path=f"""C:\\Users\\shivanandk\\Desktop\\AirflowCode\\Practise_SK\\laxmi.xlsx"""
    # dft.to_excel(file_path)
    # dfw.to_sql(table_name, connection , if_exists='replace', index=False)
    print("Completed..")

    # return outputs_of_mst
mst_coxn()                                                                                                   
