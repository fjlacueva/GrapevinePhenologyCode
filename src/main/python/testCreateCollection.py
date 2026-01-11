import requests
import json
from datetime import datetime
from requests.auth import HTTPBasicAuth
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

dateTimeObj = datetime.now()
currentDate= dateTimeObj.strftime("%Y%b%d_%H%M")



registroCorrecto = {
    "codigo": currentDate,
    "descripcion": "test " + currentDate
}

registroIncorrecto = {
    "codigoMalo": currentDate,
    "descripcion": "test " + currentDate
}

# registroIncorrecto2 = {
#     "codigo": currentDate,
#     "descripcionmala": "test " + currentDate
# }


serverURL = "***.***.226.78"
port = "8894"
service = "agrolake/insertOne"
method = "post"
headers = {'content-type': 'application/json'}
serviceURl="https"+"://"+serverURL+":"+port+"/"+service
serviceUser= "agrolakeGUI"
servicePassword= "PRO19_0181!"

##############################################################################################
# Test 1:  crear base de datos y colección con datos buenos
#
recordToSave = {
    "bbdd": "Test2",
    "coleccion": "TestCreaciones2",
    "registro": registroCorrecto
}

recordToSaveJSON= json.dumps(recordToSave)
print("Test 1: " + recordToSaveJSON)
response = requests.post(serviceURl, data=recordToSaveJSON, auth=HTTPBasicAuth(serviceUser, servicePassword), 
    headers=headers, verify=False)
print("Response test 1:"+ response.text + " "+ str(response.status_code))

##############################################################################################
# Test 2:  crear base de datos y colección con datos malos
#
recordToSave = {
    "bbdd": "Test3",
    "coleccion": "TestCreaciones2",
    "registro": registroIncorrecto
}

recordToSaveJSON= json.dumps(recordToSave)
print("Test 2: " + recordToSaveJSON)
response = requests.post(serviceURl, data=recordToSaveJSON, auth=HTTPBasicAuth(serviceUser, servicePassword), 
    headers=headers, verify=False)
print("Response test 2:"+ response.text + " "+ str(response.status_code))
##############################################################################################
# Test 3:  insertar datos buenos
#
recordToSave = {
    "bbdd": "Test4",
    "coleccion": "TestCreaciones",
    "registro": registroCorrecto
}

recordToSaveJSON= json.dumps(recordToSave)
print("Test 3: " + recordToSaveJSON)
response = requests.post(serviceURl, data=recordToSaveJSON, auth=HTTPBasicAuth(serviceUser, servicePassword), 
    headers=headers, verify=False)
print("Response test 3:"+ response.text + " "+ str(response.status_code))

##############################################################################################
# Test 4:  insertar datos malos 
#
recordToSave = {
    "bbdd": "Test",
    "coleccion": "TestCreaciones",
    "registro": registroIncorrecto
}

recordToSaveJSON= json.dumps(recordToSave)
print("Test 4: " + recordToSaveJSON)
response = requests.post(serviceURl, data=recordToSaveJSON, auth=HTTPBasicAuth(serviceUser, servicePassword), 
    headers=headers, verify=False)
print("Response test 4:"+ response.text + " "+ str(response.status_code))


# ##############################################################################################
# # Test 5:  insertar datos malos 
# #
# recordToSave = {
#     "bbdd": "Test",
#     "coleccion": "TestCreaciones2",
#     "registro": registroIncorrecto2
# }

# recordToSaveJSON= json.dumps(recordToSave)
# print("Test 5: " + recordToSaveJSON)
# response = requests.post(serviceURl, data=recordToSaveJSON, auth=HTTPBasicAuth(serviceUser, servicePassword), 
#     headers=headers, verify=False)
# print("Response test 5:"+ response.text + " "+ str(response.status_code))