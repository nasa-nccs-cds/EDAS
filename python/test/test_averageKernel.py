from pyedas.portal.edas import *

request_port = 5670
response_port = 5671
edas_server = "10.71.9.11"
# edas_server = "198.120.209.96"

portal = EDASPortal( ConnectionMode.CONNECT, edas_server, request_port, response_port )
response_manager = portal.createResponseManager()
domain = {"name":"d0"}

def testOnePointCDSparkAvg():
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
            },
            "time":{
                "start":0,
                "end":20,
                "system":"indices"
            },
            "level":{
                "start":0,
                "end":5,
                "system":"indices"
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
        }]
    ]"""
    print('Query:\n' + datainputs)

    rId1 = portal.sendMessage("execute", ["WPS", datainputs, '{ "response":"object" }'])
    responses = response_manager.getResponseVariables(rId1);
    # """:type : list[str] """
    # response = "\n".join(responses);
    # """:type : str """
    # print "Got responses:\n" + response[:300]
    data = responses[0]
    assert data.shape == (21, 6, 1, 1)
    print "Test Completed, response data shape = " + str(data.shape)


testOnePointCDSparkAvg()

portal.shutdown()
