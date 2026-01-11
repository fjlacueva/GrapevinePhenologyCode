import sys
import os
######################
## Utilities
######################
def importModule(moduleName):
    if moduleName in sys.modules:
        return sys.modules[moduleName]
    else:
        __import__(moduleName)
        return sys.modules[moduleName]
def tryGetVariable(module, fullVarName, defValue):
  varParts=fullVarName.split('.')
  varRoot=module
  base=None
  for varPart in varParts:
    dict=None
    isRootDict=type(varRoot)==type({})
    if isRootDict:
      dict=varRoot
    else:
      dict=dir(varRoot)
    if varPart in dict:
      varRoot= varRoot[varPart] if isRootDict else getattr(varRoot, varPart)
    else:
      return defValue
  return varRoot

def envBoolValue( envName, defValue:bool='False'):
  envValue=os.getenv( envName, str(defValue))
  return envValue.casefold() in ['true', '1']
