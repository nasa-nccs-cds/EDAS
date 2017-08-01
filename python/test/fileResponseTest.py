from pyedas.portal.edas import *
import time, sys, cdms2, os
from pyedas.portal.edas import *

startServer = False
portal = None
request_port = 5670
response_port = 5671
host = "cldra"
server = "localhost"

try:

    portal = EDASPortal(ConnectionMode.CONNECT, server, request_port, response_port)
    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0","lat":{"start":0,"end":90,"system":"values"},"lon":{"start":30,"end":120,"system":"values"}}],variable=[{"uri":"collection:/cip_merra2_mon_tas","name":"tas:v1","domain":"d0"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()

    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, '{ "response":"file" }'] )
    responses = response_manager.getResponseVariables(rId)
    timeSeries = responses[0](squeeze=1)
    timeSeries -= 273.15


except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


