from pyedas.portal.edas import *
import time, sys, cdms2, os, EzTemplate
import matplotlib.pyplot as plt
import datetime, matplotlib

# assumes the java/scala side of the EDASPortal has been started using the startupEDASPortal.py script.


request_port = 5670
response_port = 5671
edas_server = "localhost"

try:
    portal = EDASPortal(  edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":5,"end":45,"system":"values"},"level":{"start":0,"end":0,"system":"indices"}}],variable=[{"uri":"file:///Users/tpmaxwel/.edas/cache/collections/NCML/MERRA_DAILY.ncml","name":"t:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"file" }'] )
    fileResponses = response_manager.getResponseVariables(rId1)

    print "PLotting " + str(len(fileResponses)) + " responses"
    fileVar = fileResponses[0](squeeze=1)

    timeAxis = fileVar.getTime()
    data = fileVar.data
    list_of_datetimes = [ datetime.datetime(x.year,x.month,x.day,x.hour,x.minute,int(x.second)) for x in timeAxis.asComponentTime() ]
    dates = matplotlib.dates.date2num(list_of_datetimes)

    plt.plot_date(dates,data)
    plt.gcf().autofmt_xdate()
    plt.show()

finally:
    portal.shutdown()


