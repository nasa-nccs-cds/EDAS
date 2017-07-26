from pyedas.portal.edas import *

request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":0,"end":80,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_mon_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","axes":"yt","id":"v1ave"},{"name":"CDSpark.diff2","input":"v1,v1ave","domain":"d0"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)

    print "Received " + str(len(responses)) + " responses"
    print " Result data shape: " + str( responses[0].shape )

finally:
    portal.shutdown()

