from pyedas.portal.edas import *
import time, numpy as np

request_port = 5670
response_port = 5671
edas_server = "127.0.0.1"

try:
    log_file  = open( '/tmp/EDAS_init.log', 'w' )
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    rId = portal.sendMessage("getCapabilities", [""])
    log_file.write( "getCapabilities request sent\n" ); log_file.flush()
    responses = response_manager.getResponses(rId)
    log_file.write( "Capabilities:\n" + "\n".join(responses) ); log_file.flush()

    rId = portal.sendMessage("getCapabilities", ["coll"])
    log_file.write( "getCapabilities [ coll ] request sent\n" ); log_file.flush()
    responses = response_manager.getResponses(rId)
    log_file.write( "\n\nList Collections:\n" + "\n".join(responses) ); log_file.flush()

finally:
    log_file.write( "shutdown" ); log_file.flush()
    portal.shutdown()
    log_file.write( "close" ); log_file.flush()
    log_file.close()






