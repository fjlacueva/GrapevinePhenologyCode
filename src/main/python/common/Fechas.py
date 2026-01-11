def getDistinctDates(listDates):

    fechas = []
    cantidad = 0
    values = {}
    for date in listDates:
        fecha = date[0:10]
        if fecha not in fechas:
            if fechas == []:
                fechas.append(fecha)
                cantidad = cantidad + 1
            else:
                values[actualFecha] = cantidad
                fechas = []
                cantidad = 1
        else:
            cantidad = cantidad + 1
            actualFecha = fecha

    values[actualFecha] = cantidad

    return values