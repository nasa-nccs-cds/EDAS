from pyedas.portal.edas import *
import time

request_port = 5670
response_port = 5671

try:
    portal = EDASPortal(  "10.71.9.11", request_port, response_port )
    response_manager = portal.createResponseManager()

    datainputs = """[
        domain=[{
            "name":"d0",
            "lat":{
                "start":40.25,
                "end":40.25,
                "system":"values"
            },
            "lon":{
                "start":89.75,
                "end":89.75,
                "system":"values"
            }
        }],
        variable=[{
            "uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_6hr_hur.ncml",
            "name":"hur",
            "domain":"d0"
        }],
        operation=[{
            "name":"CDSpark.average",
            "input":"hur",
            "domain":"d0",
            "axes":"xy"
        }] ]"""

    rId = portal.sendMessage("execute", [ "WPS", datainputs, '{ "response":"object" }'] )
    responses = response_manager.getResponses(rId);   """:type : list[str] """
    response = "\n".join(responses);   """:type : str """
    print "Got responses:\n" + response[:300]

finally:
    portal.shutdown()


