import pandas as pd
from datetime import date

class DatesHandler:
    """
        Class for handling date based dataframes.
        --------------
        - Class Attributes
        -  mandatoryParameters=["beginDate", "endDate"]
        --------------
        - Instance Attributes:
        -  parameters
        -  numberOfDays: >=0, number of dates between beginDate and endDate
        -  df:  if created the dataframe.
    """
    mandatoryParameters=["beginDate", "endDate"]
    def __init__(self, parameters):
        self.parameters = parameters
        if not self.parameters.areAllMandatoryParametersPresent():
            raise Exception("DatesHandler.__init__: not all mandatoryParameters ("+ 
                    str(self.mandatoryParameters)+ ")) are set.")
        
        self.beginDate=self.parameters.getParameter("beginDate")
        if not isinstance(self.beginDate,date):
            raise Exception("DatesHandler.__init__: parameter beginDate is not a valid date.")
        self.endDate=self.parameters.getParameter("endDate")
        if not isinstance(self.endDate,date):
            raise Exception("DatesHandler.__init__: parameter endDate is not a valid date.")
        self.numberOfDays = self.endDate-self.beginDate
        if self.numberOfDays.days<=0:
            raise Exception("DatesHandler.__init__: endDate parameter: "+ str(self.endDate)+ 
                " must be greater than beginDate parameter: "+ str(self.beginDate)+".")
        self.df=None
    
    def createDataFrame(self):
        df = pd.DataFrame({ 'dayOrdinal' : range(0, self.numberOfDays.days ,1),
                    'gregorianDate':pd.date_range(self.beginDate, periods=self.numberOfDays.days)})

        df['year'] = pd.DatetimeIndex(df['gregorianDate']).year
        df['julianDate'] = df['gregorianDate'].dt.strftime('%Y%j')
        df['julianDay'] = df['gregorianDate'].dt.strftime('%j')
        df["julianDay"] = df["julianDay"].astype(str).astype(int)

        self.df = df
    
