from pyedas.portal.edas import *
import time

request_port = 5670
response_port = 5671
server = "10.71.9.11"
# server = "localhost"

try:
    portal = EDASPortal( server, request_port, response_port )
    response_manager = portal.createResponseManager()

    datainputs = """[domain=[{"name":"d0","time":{"start":0,"end":10000,"system":"indices"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.max","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"xml" }' ] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:
    portal.shutdown()


