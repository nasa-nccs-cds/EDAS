from pyedas.portal.edas import *
import matplotlib.pyplot as plt
import datetime, matplotlib

# assumes the java/scala side of the EDASPortal has been started using the startupEDASPortal.py script.


request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[   domain=[
                            {"name":"d0","time":{"start":"1980-01-01T00:00:00","end":"1980-12-31T23:00:00","system":"values"}}
                        ],
                        variable=[
                            {"uri":"collection:/ecmwf_ifs-cy31r2_6hr_ta","name":"ta:v1","domain":"d0"},
                            {"uri":"collection:/geos-5_merra_6hr_ta","name":"ta:v2","domain":"d0"},
                            {"uri":"collection:/geos-5_merra2_6hr_ta","name":"ta:v3","domain":"d0"},
                            {"uri":"collection:/jma_jra-55_6hr_ta","name":"ta:v4","domain":"d0"}
                        ],
                        operation=[
                            {"name":"CDSpark.compress","id":"cmp","input":"v1,v2,v3,v4","plev":"100000, 97500, 95000, 92500, 90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000, 55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000" },
                            {"name":"python.cdmsModule.regrid","id":"rg","input":"cmp","domain":"d0","grid":"uniform","target":"v4"}
                        ]
                    ]"""

    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "period":"month" }' ] )
    responses = response_manager.getResponses(rId1)
    vars = response_manager.getResponseVariables(rId1)

    print "Received " + str(len(responses)) + " responses"
    print "Received " + str(len(vars)) + " vars"

finally:
    portal.shutdown()


