import json

class Parameters:
    """
        Parameters class is a "generic" class to be used for all the classes in order to avoidind code
        dupplication.  Basically it implements a dictionary with verification rules.
        A parameter(or argument) is a (name, value) pair:
          - name: a valid string to be a python dictionary key
          - value: any python valid object.  It's responsability of the user to verify its correctnes.
        Implemented methods (see specific method docs for details):
          - setParameter (name, value)
          - getParameter (name)-> value
          - parametersToJSON()-> JSONstring
          - addMandatoryParameter(name)
          - isMandatoryParameter(name)-> bool
          - addMandatoryParameters(names[])
          - areAllMandatoryParametersPresent()->bool


    """
    def __init__(self):
        self.parameters = {}
        self.mandatoryParameters = []
    
    def setParameter(self, name:str=None, value:object=None):
        """
            If name is a valid not empty string it set a parameter with the value
        """
        if (name is None):
            raise Exception("Parameters.setParameter: name argument should be a not empty string.")
        if not (isinstance(name, str)):
            raise Exception("Parameters.setParameter: name argument should be a not empty string.")
        if len(name)==0:
            raise Exception("Parameters.setParameter: name argument should be a not empty string.")

        self.parameters[name]=value
    
    def getParameter(self, name:str=None)->object:
        """
            If name is a valid not empty string it and it exist a parameter with it as name, getParamater return the value.
        """
        value = None

        if self.isDefinedParameter(name):
            value = self.parameters[name]
        else:
            raise Exception("Parameters.getParameter: name argument"+  name+ "is not included in parameter keys.")
        return value
    
    def parametersToJSON(self)->str:
        """
            Returns a JSON string with the current values of the parameters
        """
        jsonParameters = None
        jsonParameters = json.dumps(self.parameters, sort_keys = True, default = str)
        return jsonParameters

    def addMandatoryParameter(self, name:str=None):
        """
            It sets name as a mandatoryParameter: it has to be defined and has a value different of None.
            Name has to be a non empty string.
        """
        if (name is None):
            raise Exception("Parameters.addMandatoryParameter: name argument should be a not empty string.")
        if not (isinstance(name, str)):
            raise Exception("Parameters.addMandatoryParameter: name argument should be a not empty string.")
        if len(name)==0:
            raise Exception("Parameters.addMandatoryParameter: name argument should be a not empty string.")

        self.mandatoryParameters.append(name)

    def isMandatoryParameter(self, name:str=None)->bool:
        """
            It returns True if name identifies a parameter considered mandatory.
        """
        isMandatory = False
        if (name is None):
            raise Exception("Parameters.isMandatoryParameter: name argument should be a not empty string.")
        if not (isinstance(name, str)):
            raise Exception("Parameters.isMandatoryParameter: name argument should be a not empty string.")
        if len(name)==0:
            raise Exception("Parameters.isMandatoryParameter: name argument should be a not empty string.")

        if name in self.mandatoryParameters.keys():
            isMandatory=True
        return isMandatory
    
    def addMandatoryParameters(self, names:list=None):
        """
            It sets all elementes of names as a mandatoryParameter: it has to be defined and has a value different of None.
            Names has to be a non empty list of non empty string.
        """

        if (names is None):
            raise Exception("Parameters.addMandatoryParameter: names argument should be a not empty string list.")

        for name in names:
            if not (isinstance(name, str)):
                raise Exception("Parameters.addMandatoryParameters: names argument should be a not empty string.  It contains at least one object is not.")
            if len(name)==0:
                raise Exception("Parameters.addMandatoryParameters: name argument should be a not empty string. It contains at least one object is not.")
            self.addMandatoryParameter(name)

    def areAllMandatoryParametersPresent(self)->bool:
        """
            Returns true if all the self.mandatoryParameters elements are keys of self.parameters and their values are different of None.
            It does not verify the validity of the values, user code is responssible of it.
        """
        arePresent=True
        for mandatoryParameter in self.mandatoryParameters:
            if not mandatoryParameter in self.parameters.keys():
                arePresent = False
            else:
                value = self.parameters[mandatoryParameter]
                if (value is None):
                    arePresent = False
        return arePresent

    def isDefinedParameter(self, name:str=None)->bool:
        """
            returns True if name is a parameter
        """
        isDefined= False
        if not isinstance(name,str):
            raise Exception("Parameters.isDefinedParameter: name argument should be a not empty string.")
        if (name is None) or len(name)==0:
            raise Exception("Parameters.addMandatoryParameter: names argument should be a not empty string list.")

        if name in self.parameters.keys():
            isDefined= True
        return isDefined
