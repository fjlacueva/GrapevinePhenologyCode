import requests
import re
import requests 

class InvoqueAirMapWS:
    def __init__(self, parameters):
        if (parameters is None):
            raise Exception("InvoqueAirMapWS.__init__: parameters param is not established.  Please review your code.")
        
        if ("serverURL" not in parameters.keys()):
            raise Exception("InvoqueAirMapWS.__init__: parameters does not contain 'serverURL'.  Please review your code.")
        if ("Accept" not in parameters.keys()):
            raise Exception("InvoqueAirMapWS.__init__: parameters does not contain 'Accept'.  Please review your code.")
        if ("X-API-Key" not in parameters.keys()):
            raise Exception("InvoqueAirMapWS.__init__: parameters does not contain 'X-API-Key'.  Please review your code.")
        
        self.serverURL=parameters['serverURL']
        if ((self.serverURL is None ) or (len(self.serverURL)==0)):
            raise Exception("InvoqueAirMapWS.__init__: parameters['serverURL'] does not contain a valid value.  Please review your code.")
        self.Accept=parameters['Accept']
        if ((self.Accept is None ) or (len(self.Accept)==0)):
            raise Exception("InvoqueAirMapWS.__init__: parameters['Accept'] does not contain a valid value.  Please review your code.")
        self.X_API_Key=parameters['X-API-Key']
        if ((self.X_API_Key is None ) or (len(self.X_API_Key)==0)):
            raise Exception("InvoqueAirMapWS.__init__: parameters['X-API-Key'] does not contain a valid value.  Please review your code.")
        self.coordinatePattern =r'[+\-]*[0-9]?\.*[0-9]*'
        self.coordinatePatternC = re.compile(self.coordinatePattern)

        self.headers ={
            "Accept": self.Accept,
            'X-API-Key': self.X_API_Key
        }

    def getPointsAltitude(self, points:str=None):
        response =None
        if ((points is None) or (len(points)==0)):
            raise(Exception("InvoqueAirMapWS.getPointsAltitude: points parameter must be established. Please, review it."))
        splitPoints= points.split(",")
        if len(splitPoints)%2 !=0 :
            raise(Exception("InvoqueAirMapWS.getPointsAltitude: points parameters must provide an even number of coordinates."))
        for i in range(len(splitPoints)):
            matched=self.coordinatePatternC.match(splitPoints[i])
            if (matched is None):
                raise(Exception("InvoqueAirMapWS.getPointsAltitude: points parameters must provide an even number of coordinates."))

        # headers = {
        #     "Accept": "application/json; charset=utf-8",
        #     'X-API-Key':'{eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJjcmVkZW50aWFsX2lkIjoiY3JlZGVudGlhbHxRRFlYYmRKc3hXNGF2YlQ2RXpFd0pGN1pta0dYIiwiYXBwbGljYXRpb25faWQiOiJhcHBsaWNhdGlvbnxkcFJ5ZFFlY0xhWHA1WlVSUW80R1hGV01hWktOIiwib3JnYW5pemF0aW9uX2lkIjoiZGV2ZWxvcGVyfFFldmQ1bHlTWWJ4eDJ0endlOG8ySEFvRWJuTCIsImlhdCI6MTU4MDQ3OTYyMn0.kXQt58TBYOqZCvyzJWHHYszaSuNDMHBoOM_9znnkoig}'
        # }

        params = {"points": points}
        try:
            response = requests.get(url = self.serverURL, params = params, headers=self.headers) 
        except Exception as e:
            altitudesjson=[-1]
        else:
            if response.status_code!=200:
                raise(Exception("InvoqueAirMapWS.getPointsAltitude: somethings goes wrong when invoquing web service. Error: "+ response.status_code +
                ". Reason: "+ response.reason+".  Please review your code"))
            altitudesjson = response.json()["data"]

        return  altitudesjson