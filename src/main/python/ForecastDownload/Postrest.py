import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def getConnection(url,auth):
    try:
        connectionTry = requests.get(url, auth = auth, verify=False)
        return 1 if connectionTry.status_code == 200 else 0
    except Exception as e:
        print(str(e))
        return -1

def getDataTable(url,table, auth):
    url = url + table
    return requests.get(url, auth = auth, verify=False)

def getDataTableWithFilter(url,table,columns,values, auth):
    url = url + table
    url = f"{url}?{columns[0]}=eq.{values[0]}"
    for position, column in enumerate(columns[1:], start=1):
        url = f"{url}&{column}=eq.{values[position]}"
    return requests.get(url, auth = auth, verify=False)

def insertRow(url,table,data, auth):

    headers = {'Content-type': 'application/json'}
    url = url + table
    answer = requests.post(url,data=json.dumps(data),headers=headers,auth = auth, verify=False)
    return answer

def deleteRow(url,table,columns,values, auth):

    url = url + table
    url = f"{url}?{columns[0]}=eq.{values[0]}"
    for position, column in enumerate(columns[1:], start=1):
        url = f"{url}&{column}=eq.{values[position]}"
    answer = requests.delete(url, auth = auth, verify=False)
    return answer