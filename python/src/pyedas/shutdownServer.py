from pyedas.portal.edas import *

request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal(ConnectionMode.CONNECT, edas_server, request_port, response_port)
    rId = portal.sendMessage( "quit", [] )

except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()



