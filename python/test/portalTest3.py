from pyedas.portal.edas import EDASPortal
import time

try:
    portal = EDASPortal()
    response_manager = portal.getResponseManager()
    portal.start_EDAS()
    time.sleep(4)
    rId = portal.sendMessage("getCapabilities", [ "WPS" ] )
#    rId = portal.sendMessage("describeProcess", [ "numpymodule.ptp" ] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:

    portal.shutdown()