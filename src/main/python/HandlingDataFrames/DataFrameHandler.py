import pandas as pd
from sklearn import preprocessing
import datetime 
import os
import re
import copy


class DataFrameHandler:
    def __init__(self, parameters:dict=None):
        self.parameters = parameters
        if not self.parameters.areAllMandatoryParametersPresent():
            raise Exception("DataFrameHandler.__init__: not all mandatoryParameters ("+ 
                    str(self.mandatoryParameters)+ ")) are set.")
    
    def readDFFromCSVFile(self, filePath:str=None, separator:str=";")->object:
        """
            It verifies filePath is the path to and excel file and returns its first sheet as 
            pandas data frame
        """
        df = None
        if not isinstance(filePath, str):
            raise Exception("DataFrameHandler.readDFFromCSVFile: filePath is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("DataFrameHandler.readDFFromCSVFile: filePath has no value.")
        if not os.path.isfile(filePath):
            raise Exception("DataFrameHandler.readDFFromCSVFile: filePath parameter is not a valid file path:"+ filePath)
        filename, file_extension = os.path.splitext(filePath)
        file_extension=file_extension.lower()
        if file_extension!=".csv":
             raise Exception("DataFrameHandler.readDFFromCSVFile: filePath parameter does not point to an Excel file:"+ filePath)
        
        if (separator is None) or (len(separator)==0):
            separator=";"
        df = pd.read_csv(filepath_or_buffer=filePath, sep=separator)

        return df
    
    def readDFFromExcelFile(self, filePath:str=None)->object:
        """
            It verifies filePath is the path to and excel file and returns its first sheet as 
            pandas data frame
        """
        df = None
        if not isinstance(filePath, str):
            raise Exception("DataFrameHandler.readDFFromExcelFile: filePath is no a valid string.")
        if (filePath is None) or (len(filePath)==0):
            raise Exception("DataFrameHandler.readDFFromExcelFile: filePath has no value.")
        if not os.path.isfile(filePath):
            raise Exception("DataFrameHandler.readDFFromExcelFile: filePath parameter is not a valid file path:"+ filePath)
        filename, file_extension = os.path.splitext(filePath)
        file_extension=file_extension.lower()
        if file_extension!=".xlsx" and file_extension!=".xls":
             raise Exception("DataFrameHandler.readDFFromExcelFile: filePath parameter does not point to an Excel file:"+ filePath)
        df = pd.read_excel(filePath)

        return df
    
    def readDFFromExcelPrefixedFile(self, filePath:str=None, fileName:str=None, prefix:str=None)->object:
        """
            if fileName stars with prefix, it reads the Excel content and returns as df. If there is not 
            matching None is returned
        """
        df = None
        if not isinstance(filePath, str):
            raise Exception("DataFrameHandler.readDFFromExcelPrefixedFile: filePath is no a valid string.")
        if (prefix==None) or (len(prefix)==0):
            df = self.readDFFromExcelFile(filePath+fileName)
        else:
            if (fileName.startswith(prefix)):
                if not (filePath.endswith(os.path.sep)):
                    filePath = filePath + os.path.sep
                df = self.readDFFromExcelFile(filePath+fileName)
        return df

    def createOutputFileNamePath (self, filePath=None,fileName:str=None, typeOfSave:str="csv", fileSeparator:str=";"):
        """
            Returns the path of the fileName considering invoquing argument and
                self.parameters.getParameter("diaryObservationsPathsAndPatterns")
            If patha exists and fileName is a valid string and fileSeparator is a valid string (character)
            it returns the path according to definition.  Otherwise an exception is raised.
        """
        #filePath = None
        if ((fileName is None) or (len(fileName)==0)):
            raise Exception("DataFrameHandler.createOutputFileNamePath: fileName ")

        if (fileSeparator is None) or len(fileSeparator)==0:
            fileSeparator=";"
        pathsAndPatterns = self.parameters.getParameter("diaryObservationsPathsAndPatterns")
        if not isinstance(pathsAndPatterns, str):
            raise Exception("DataFrameHandler.createOutputFileNamePath: diaryObservationsPathsAndPatterns is no a valid string.")
        if (pathsAndPatterns is None) or (len(pathsAndPatterns)==0):
            raise Exception("DataFrameHandler.createOutputFileNamePath: diaryObservationsPathsAndPatterns is no a valid string.")

        splitPathsAndPatterns=pathsAndPatterns.split(self.pathAndPatternsSeparator)
        if (len(splitPathsAndPatterns)!=2):
            raise Exception("DataFrameHandler.createOutputFileNamePath: diaryObservationsPathsAndPatterns paremeter should have the format <pathToFolder>;<Prefix>.")

        if (filePath is None) or (len(filePath)==0):
            folderPath = splitPathsAndPatterns[0]
        else:
            folderPath = filePath
        prefix = splitPathsAndPatterns[1]
        if not(os.path.isdir(folderPath)):
            raise Exception("DataFrameHandler.createOutputFileNamePath: diaryObservationsPathsAndPatterns path paremeter does not point to an accesible folder:"+ folderPath)

        if not folderPath.endswith(os.path.sep):
            filePath=folderPath+os.path.sep+fileName
        else:
            filePath=folderPath+fileName

        return filePath

    def saveDataFrameToCsv(self, df, filePath:str=None, fileSeparator:str=";")->bool:
        saved = True

        if (filePath is None) or (len(filePath)==0):
            raise Exception("DataFrameHandler.saveDataFrameToCsv: filePath param does not have a valid.")
 
        df.to_csv(path_or_buf=filePath,sep=fileSeparator,index=False)

        return saved

    def createEmptyClimaticStationDataFrame(self)->object:
        """
            creates an empty Data Frame
        """
        df = pd.DataFrame(columns=self.columns)
        df = df.fillna(0) # with 0s rather than NaNs
        return df
    
    def renameDataFrameColumns(self, dataFrame=None)->bool:
        """
            For each column in dataFrame:
            * If the column name is compossed it rename columna as <name1>_<name2>
            * If not it is renamed to name1
            Returns True if renamed if not an execption is raised.
        """
        columnNumber=0
        index={}
        columns={}
        columnsList=[]
        for columnName in dataFrame.columns:
            #columnName = columnName.split(",")
            newName=None
            if len(columnName)==2:
                name1= columnName[0]
                name2= columnName[1]
                if len(name2)>0:
                    newName=name1+"_"+name2
                else:
                    newName=name1
            else:
                newName=columnName[0]
            index[columnNumber]=newName
            columns[columnName]=newName
            columnsList.append(newName)
            columnNumber+=1
        dataFrame.columns=columnsList
        return dataFrame
    
    def convertColumnsToInteger(self, dataframe=None, columns:list=None):
        for key in columns:
            dataframe[key] = dataframe[key].astype(int, copy=True, errors='raise')
        return dataframe

    def convertColumnsToFloat(self, dataframe=None, columns:list=None):
        for key in columns:
            dataframe[key] = dataframe[key].astype(float, copy=True, errors='raise')
        return dataframe

    def convertColumnsToDatetime(self, dataframe=None, columns:list=None):
        for key in columns:
            dataframe[key] = pd.to_datetime(dataframe[key])
        return dataframe

    def convertColumnsToTime(self, dataframe=None, columns:list=None):
        for key in columns:
            dataframe[key] = pd.to_datetime(dataframe[key])
        return dataframe

    def getSelectedColumnsDF(self, dataframe = None, selectedColumns: list= None):
        copyDF = None
        if (dataframe is None) or len(dataframe.index)==0:
            raise Exception("DataFrameHandler.getSelectedColumnsDF: dataframe paremeter is None or it does not have any row.")

        dataFrameColumns=dataframe.columns
        if (selectedColumns==None):
            selectedColumns=dataFrameColumns

        copyDF= dataframe.copy()

        # removing not selectedColumns From copyDF
        for column in dataFrameColumns:
            if column not in selectedColumns:
                copyDF.drop(column, inplace=True, axis=1)

        return copyDF
    
    def getColumnsCorrelation(self, dataframe=None):
        correlationMatrix = None

        if (dataframe is None) or len(dataframe.index)==0:
            raise Exception("DataFrameHandler.getColumnsCorrelation: dataframe paremeter is None or it does not have any row.")

        # creating correlation matrix
        correlationMatrix = dataframe.corr()

        return correlationMatrix

    def getImplementedNormalizationMethods(self)->list:
        implementedMethods=["min_max_scaler"]
        return implementedMethods
    
    def normalizeDataFrame (self, dataframe = None, 
            notNormalizedColumns : list = None,
            normalizationMethod:str="min_max_scaler"):

        normalizedDF = None

        if normalizationMethod is None:
            normalizationMethod ="min_max_scaler"
        
        if (normalizationMethod not in self.getImplementedNormalizationMethods()):
            raise Exception("DataFrameHandler.normalizeDataFrame: normalizationType paremeter value("+ str(normalizationMethod)+\
                ")  is not in implemeted ones. Take a look to DataFrameHandler.getImplementedNormalizationMethods() returned values.")
        
        columns = dataframe.columns
        columnsToNormalize = []
        for column in columns:
            if not (column in notNormalizedColumns):
                columnsToNormalize.append(column)
        restrictedDF = self.getSelectedColumnsDF(dataframe=dataframe,selectedColumns=columnsToNormalize)
        
        if (normalizationMethod=="min_max_scaler"):
            min_max_scaler = preprocessing.MinMaxScaler()
            scaledNP = min_max_scaler.fit_transform(restrictedDF)
            normalizedDF = pd.DataFrame(scaledNP)
        
        # "Denormalizing" not desidered columns
        for column in notNormalizedColumns:
            if column in columns:
                normalizedDF[column]=dataframe[column]
        return normalizedDF

