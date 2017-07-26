from pyedas.portal.edas import *
import time, numpy as np
import matplotlib.pyplot as plt

request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = """[domain=[{"name":"d0","lat":{"start":0,"end":80,"system":"values"}}],variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_mon_tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","axes":"yt","id":"v1ave"},{"name":"CDSpark.diff2","input":"v1,v1ave","domain":"d0"}]]"""
    t0 = time.time()
    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)

    print "Received " + str(len(responses)) + " responses"
    print " Result data shape: " + str( responses[0].shape )
    print "Completed OP in time {0}".format( time.time()-t0 );

    time_index = 0
    timeslice = responses[0].subSlice( time_index, ":", ":" ).data

    plt.imshow(timeslice)

finally:
    portal.shutdown()

