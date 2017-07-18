from pyedas.portal.edas import EDASPortal
import time

try:
    portal = EDASPortal()
    response_manager = portal.createResponseManager()
    portal.start_EDAS()
    time.sleep(4)

    rId = portal.sendMessage("getCapabilities", [ "" ] )
    responses = response_manager.getResponses(rId)
    print "Got responses:\n" + "\n".join(responses)

finally:

    portal.shutdown()



