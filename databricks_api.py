# import requests
# api_url = "https://bolt-analyticsprod.cloud.databricks.com/api/2.0/workspace/get-status"
# data = {   "cluster_id": "0914-072437-cwq0vku"}#, "notebook_task": {  "notebook_path": "/Shared/python-test-SK"  } } #/Shared/python-test-SK
# headers = { "Authorization": "Bearer dapi3a452c566de3b6e1a86ae99d20945c5e"} 
# response = requests.get(api_url, json=data, headers=headers)
# print(response)
# if response.status_code == 200:
#     print(response.text)
# else:   print("Error submitting notebook run:", response.text)

import time
import json
import requests
databricks_url = "https://bolt-analyticsprod.cloud.databricks.com/api/1.2"
token = "dapi883353a8993f4471071d960221b2bd91"#"dapi3a452c566de3b6e1a86ae99d20945c5e"
notebook_path = "/Shared/python-test-SK"

# api_endpoint = f"{databricks_url}/workspace/get-status"
api_endpoint = f"{databricks_url}/commands/status"  #list#import#commands/status
# api_endpoint = f"{databricks_url}/notebook/2187035764759036" 

headers = {
    "Authorization": f"Bearer {token}",
}
data={
  "clusterId": "0914-072437-cwq0vku",
  "contextId": "835052045213985472",#6118225353876631607
#   "language": "sql",
#   "command": "select count(*) from adp_core.dl2.policy"
    "commandId":"1d24e69815e34c4a9282cf3b642426ac"#"171bba3c5a6d4bcfb76c9e12b718df0d"
}
params = {
    "path": notebook_path,
    "format": "SOURCE",  
}
response = requests.get(api_endpoint, headers=headers, params=data)

def run_a_command():
    api_endpoint = f"{databricks_url}/commands/execute"  #list#import#commands/status

    data={
  "clusterId": "0914-072437-cwq0vku",
  "contextId": "835052045213985472",#6118225353876631607
  "language": "sql",
  "command": "select * from adp_core.dl2.policy"
        }
    response = requests.post(api_endpoint, headers=headers, data=data)



    if response.status_code == 200:
        # Notebook content
        notebook_content = response.text
        # json_string = '{"id": "a65664f2a0384a25a6c82fe5802b3573"}'
        json_object = json.loads(notebook_content)
        global id_value
        id_value = json_object["id"]
        print(f"Notebook content for {notebook_path}:\n{id_value}")
    else:
        print(f"Failed to retrieve notebook information. Status code: {response.status_code}")
        print(f"Response content: {response.text}")

run_a_command()
time.sleep(15)

def command_info():
    api_endpoint = f"{databricks_url}/commands/status"  #list#import#commands/status

    data={
    "clusterId": "0914-072437-cwq0vku",
    "contextId": "835052045213985472",#6118225353876631607
    # "language": "sql",
    "commandId": id_value
            }
    response = requests.get(api_endpoint, headers=headers, params=data)

    if response.status_code == 200:
        # Notebook content
        notebook_content = response.text
        # # json_string = '{"id": "a65664f2a0384a25a6c82fe5802b3573"}'
        # json_object = json.loads(notebook_content)
        # global id_value
        # id_value = json_object["id"]
        print(f"Notebook content for {notebook_path}:\n{notebook_content}")
    else:
        print(f"Failed to retrieve notebook information. Status code: {response.status_code}")
        print(f"Response content: {response.text}")

command_info()




