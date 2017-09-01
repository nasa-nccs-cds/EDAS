from pyedas.portal.edas import *
import matplotlib.pyplot as plt
import datetime, matplotlib

# assumes the java/scala side of the EDASPortal has been started using the startupEDASPortal.py script.


request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal(  edas_server, request_port, response_port )
    response_manager = portal.getResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":5,"end":45,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"python.numpyModule.ave","input":"v1","domain":"d0","axes":"xy"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"file" }'] )
    fileResponses = response_manager.getResponseVariables(rId1)

    print "PLotting " + str(len(fileResponses)) + " responses"
    fileVar = fileResponses[0](squeeze=1)
    data = fileVar.data
    print "Received response data, size =  " + str(data.shape)

    fid = open( "/tmp/edas_test_data-" + str(time.time()) + ".txt","w")
    data.tofile( fid, " ", "%.2f" )

finally:
    portal.shutdown()


