from pyedas.portal.edas import *
import matplotlib.pyplot as plt
import datetime, matplotlib

# assumes the java/scala side of the EDASPortal has been started using the startupEDASPortal.py script.


request_port = 5670
response_port = 5671
# edas_server = "10.71.9.11"
edas_server = "localhost"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
#    datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":5,"end":45,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]"""
    datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":5,"end":45,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/MERRA_TAS1hr.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)

    print "PLotting " + str(len(responses)) + " responses"
    resultVar = responses[0](squeeze=1)

    timeAxis = resultVar.getTime()
    data = resultVar.data
    list_of_datetimes = [ datetime.datetime(x.year,x.month,x.day,x.hour,x.minute,int(x.second)) for x in timeAxis.asComponentTime() ]
    dates = matplotlib.dates.date2num(list_of_datetimes)

    plt.plot_date(dates,data)
    plt.gcf().autofmt_xdate()
    plt.show()

finally:
    portal.shutdown()


