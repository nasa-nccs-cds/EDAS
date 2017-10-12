from pyedas.portal.edas import *
import time, numpy as np

request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

def getCycle( responses, monthIndex ):
    cycle_data = np.zeros((24))
    for rvar in responses:
        elemIndex = int( rvar.attributes.get("elem","-1") )
        cycle_data[elemIndex] = rvar.data.flatten()[monthIndex]
    return cycle_data

try:
    portal = EDASPortal(  edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":25,"end":45,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/MERRA_TAS1hr.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","cycle":"diurnal","bin":"month","axes":"t"}]]"""
    t0 = time.time()
    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)
    print "Completed OP in time {0}".format( time.time()-t0 );

    cycle_data = getCycle( responses, 5 )
    print str( cycle_data )

finally:
    portal.shutdown()



