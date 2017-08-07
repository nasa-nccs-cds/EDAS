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
    datainputs = """[domain=[{"lat":{"start":8,"end":13,"system":"indices"},"lon":{"start":70,"end":72,"system":"indices"},"name":"d0","time":{"start":5,"end":10,"system":"indices"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA_mon_pr.ncml","name":"pr","domain":"d0"}],operation=[{"input":"pr","domain":"d0","name":"python.numpyModule.avew","axes":"xy"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }' ] )
    responses = response_manager.getResponses(rId1)

    print "Received " + str(len(responses)) + " responses"

    print "Response: " + str( responses[0] )

finally:
    portal.shutdown()


