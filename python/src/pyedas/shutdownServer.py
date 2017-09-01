from pyedas.portal.edas import *

request_port = 5670
response_port = 5671
edas_server = "localhost"
portal = None

try:
    portal = EDASPortal(  edas_server, request_port, response_port)
    rId = portal.sendMessage( "quit", [] )

except Exception, err:
    traceback.print_exc()

finally:

    if( portal ): portal.shutdown()



