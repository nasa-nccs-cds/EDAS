from pyedas.portal.edas import *
import matplotlib.pyplot as plt
import datetime, matplotlib

# assumes the java/scala side of the EDASPortal has been started using the startupEDASPortal.py script.


request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":8,"end":13,"system":"indices"},"lon":{"start":70,"end":72,"system":"indices"},"time":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA_mon_pr.ncml","name":"pr:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.avew","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs ] )
    responses = response_manager.getResponseVariables(rId1)

    print "Received " + str(len(responses)) + " responses"

finally:
    portal.shutdown()


