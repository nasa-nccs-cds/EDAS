from pyedas.portal.edas import *
import time, sys, cdms2, os
from pyedas.portal.edas import *
from cdms2.variable import DatasetVariable

request_port = 5670
response_port = 5671
# edas_server = "10.71.9.11"
edas_server = "198.120.209.96"

try:

    portal = EDASPortal(  edas_server, request_port, response_port)
    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0","lat":{"start":0,"end":90,"system":"values"},"lon":{"start":30,"end":120,"system":"values"},"level":{"start":20,"end":20,"system":"indices"}}],variable=[{"uri":"collection:/merra2_local3d_6hr","name":"T:v1","domain":"d0"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, edas_server, datainputs ); sys.stdout.flush()

    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, '{ "response":"file" }'] )
    responses = response_manager.getResponseVariables(rId);   """:type : list[DatasetVariable] """
    timeSeries = responses[0](squeeze=1);
    timeSeries -= 273.15

    print "Received timeSeries data: " + str( timeSeries.data )


except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


