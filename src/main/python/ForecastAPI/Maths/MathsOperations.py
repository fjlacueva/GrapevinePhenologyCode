import re

def fromDegreesMinutesSecondsToDecimal(value):

    #lat = '''51°36'9.18"N'''
    lat = value
    deg, minutes, seconds, direction = re.split('[°\'"]', lat)
    results = (float(deg) + float(minutes)/60 + float(seconds)/(60*60)) * (-1 if direction in ['W', 'S'] else 1)
    return results