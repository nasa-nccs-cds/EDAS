from pyedas.portal.edas import *
import time, numpy as np

request_port = 5670
response_port = 5671
# edas_server = "10.71.9.11"
edas_server = "localhost"
test_collection = "cip_cfsr_6hr_ta"

try:
    portal = EDASPortal(  edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    rId = portal.sendMessage("getCapabilities", ["coll"])
    responses = response_manager.getResponses(rId)
    print "\n\nList Collections:\n" + "\n".join(responses)

    rId = portal.sendMessage("getCapabilities", [ "coll:" + test_collection ])
    responses = response_manager.getResponses(rId)
    print "\n\nCollection " + test_collection + " Metadata:\n" + "\n".join(responses)


finally:
    portal.shutdown()






