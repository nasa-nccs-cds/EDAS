from pyedas.portal.edas import *
import time, sys, cdms2, os
import plotly.offline as py
import plotly.graph_objs as go
import pandas as pd

py.init_notebook_mode(connected=True)
portal = None
request_port = 5670
response_port = 5671
host = "cldra"
server = "10.71.9.11"

try:

    portal = EDASPortal(ConnectionMode.CONNECT, server, request_port, response_port)
    response_manager = portal.createResponseManager()

    t0 = time.time()
    datainputs = '[domain=[{"name":"d0","lat":{"start":5,"end":40,"system":"values"},"lon":{"start":80,"end":120,"system":"values"}}],variable=[{"uri":"http://esgf.nccs.nasa.gov/thredds/dodsC/CMIP5/NASA/GISS/historical/E2-H_historical_r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]'
    print "Sending request on port {0}, server {1}: {2}".format( portal.request_port, server, datainputs ); sys.stdout.flush()

    rId = portal.sendMessage( "execute", [ "CDSpark.workflow", datainputs, '{ "response":"object" }'] )
    objectResponses = response_manager.getResponseVariables(rId)
    timeSeries  = objectResponses[0](squeeze=1)
    timeSeries -= 273.15

    print "Plotting data"

    datetimes = pd.to_datetime(timeSeries.getTime().asdatetime())
    data = [go.Scatter(x=datetimes, y=timeSeries)]
    py.iplot(data)

except Exception, err:
    traceback.print_exc()

finally:

    portal.shutdown()


