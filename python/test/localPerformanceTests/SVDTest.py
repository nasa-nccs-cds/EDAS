from pyedas.portal.edas import *
import time, numpy as np
import matplotlib.pyplot as plt
from matplotlib import animation

request_port = 5670
response_port = 5671
#edas_server = "10.71.9.11"
edas_server = "localhost"
# data_uri = "file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_mon_tas.ncml"
# data_uri = "http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml"

try:
    portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
    response_manager = portal.createResponseManager()
    datainputs = ("""[domain=[{"name":"d0","lat":{"start":0,"end":80,"system":"values"}}],variable=[{"uri":"http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml","name":"tas:v1","domain":"d0"}]"""
                  """,operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","axes":"yt","id":"v1ave"},{"name":"CDSpark.diff2","input":"v1,v1ave","domain":"d0"}]]""")
#    datainputs = """[domain=[{"name":"d0","lat":{"start":0,"end":80,"system":"values"}}],variable=[{"uri":"http://dataserver.nccs.nasa.gov/thredds/dodsC/bypass/CREATE-IP/reanalysis/MERRA2/mon/atmos/tas.ncml","name":"tas:v1","domain":"d0"}],operation=[{"name":"CDSpark.binAve","input":"v1","domain":"d0","axes":"yt"}]]"""
    t0 = time.time()
    rId1 = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponseVariables(rId1)
    responseVar = responses[0]
    nTimeSteps = responseVar.shape[0]

    print " Result data shape: " + str( responseVar.shape ) + ", nTimeSteps = " + str( nTimeSteps )
    print "Completed OP in time {0}".format( time.time()-t0 );
    timeslice = responseVar.subSlice( 0, ":", ":" ).data.squeeze()

    fig = plt.figure()
    im = plt.imshow( timeslice, origin="lower", cmap=plt.cm.get_cmap('jet') )

    def getImage( tindex ):
        return responseVar.subSlice( tindex, ":", ":" ).data.squeeze()

    def init():
        im.set_data( getImage( 0 ) )
        return [im]

    def animate(i):
        im.set_data( getImage( i ) )
        return [im]

    anim = animation.FuncAnimation(fig, animate, init_func=init, frames=nTimeSteps, interval=250, blit=True)
#    anim.save('anomaly_animation.mp4', fps=2, extra_args=['-vcodec', 'libx264'])

    plt.show()

finally:
    portal.shutdown()

