from pyedas.portal.edas import *
import time, numpy as np

request_port = 5670
response_port = 5671
# edas_server = "10.71.9.11"
edas_server = "localhost"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    rId = portal.sendMessage("getCapabilities", [""])
    responses = response_manager.getResponses(rId)
    print "Capabilities:\n" + "\n".join(responses)

    rId = portal.sendMessage("getCapabilities", ["coll"])
    responses = response_manager.getResponses(rId)
    print "\n\nList Collections:\n" + "\n".join(responses)

finally:
    portal.shutdown()






