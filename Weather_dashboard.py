from urllib import response
import requests
import json
from databricks import sql
from datetime import datetime
 
 
# timestamp = 1553367060
# dt_obj = datetime.fromtimestamp(timestamp).strftime('%d-%m-%y')
 
# print("date:",dt_obj)
 
# print("date_time:",dt_obj)
# print("type of dt:",type(dt_obj))

def get_temp():
    response=requests.get("https://dataservice.accuweather.com/currentconditions/v1/204108?apikey=CRkShESPvOKm8EjdYgyFjNy0BmR5DVVy")
    print(response.status_code)
    responsex=response.text
    data = json.loads(responsex)
    Tempreture = data[0]["Temperature"]["Metric"]["Value"]
    Type_of_climate=data[0]["WeatherText"]
    LocalObservationDateTime=data[0]["LocalObservationDateTime"]

    print(Tempreture,
    LocalObservationDateTime,
    Type_of_climate)
    return (Tempreture,LocalObservationDateTime,Type_of_climate)

def databricks():
    host="bolt-analyticsprod.cloud.databricks.com"
    http_path="sql/protocolv1/o/3158924937735331/0525-070435-t5v0j694"#shivanand_k__cluster
    access_token="dapid0901d27ea66595762a8a5484812b458"#shivanand_k_access_token
    connection = sql.connect(
    server_hostname=host,
    http_path=http_path,
    access_token=access_token)
    return connection

def target():
   connection=databricks()
   Tempreture,LocalObservationDateTime,Type_of_climate=get_temp()
#    result=connection.cursor().execute("INSERT INTO table_name (column1 ,column2, column3, ) VALUES (value1, value2, value3)")
   res=result.fetchall()

target()