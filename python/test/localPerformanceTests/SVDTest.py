from pyedas.portal.edas import *

request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":0,"end":80,"system":"values"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CREATE-IP/reanalysis/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/tas/tas_Amon_reanalysis_MERRA2_198001-201412.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.anomaly","input":"v1","domain":"d0","axes":"yt"}]]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)

    print "Received " + str(len(responses)) + " responses"
    resultVar = responses[0](squeeze=1)

    print " Result data: " + str( resultVar.data )

finally:
    portal.shutdown()

