import datetime

initialYear = 2016
monthsYear = 12
months = range(1,monthsYear + 1)
currentDateTime = datetime.datetime.now()
date = currentDateTime.date()
actualYear = int(date.strftime("%Y"))
useYear = int(date.strftime("%Y"))
actualYear = 2023
if useYear == actualYear:
    actualMonth = date.strftime("%m")
else:
    actualMonth = 12
actualMonths = range(1,int(actualMonth)+1)