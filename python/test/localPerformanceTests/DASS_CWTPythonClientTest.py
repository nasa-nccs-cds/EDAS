""" Variable Unittest. """

import cwt, os

# datainputs = """[domain=[{"name":"d0","lat":{"start":70,"end":90,"system":"values"},"lon":{"start":5,"end":45,"system":"values"}}],
# variable=[{"uri":"file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/MERRA_TAS1hr.ncml","name":"tas:v1","domain":"d0"}],
# operation=[{"name":"CDSpark.average","input":"v1","domain":"d0","axes":"xy"}]]"""

class DemoWorkflow:
    def run( self ):
        d0 = cwt.Domain([], name="d0")

        op1 = cwt.Operation.from_dict( { 'name': "CDSpark.multiAverage" } )
        op1.add_input( cwt.Variable("file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/MERRA_TAS1hr.ncml", "tas" ) )

        op3 = cwt.Operation.from_dict( { 'name': 'CDSpark.regrid', 'crs':'gaussian~128' } )
        op3.add_input( op1 )

        op2 = cwt.Operation.from_dict( { 'name': "CDSpark.multiAverage" } )
        for i in range(1,3): op2.add_input( cwt.Variable('collection:/GISS-E2-R_r%di1p1'%(i), "tas" ) )
        op4 = cwt.Operation.from_dict( { 'name': 'CDSpark.regrid', 'crs':'gaussian~128' } )
        op4.add_input( op2 )
        op5 = cwt.Operation.from_dict( { 'name': 'CDSpark.multiAverage' } )
        op5.add_input( op3 )
        op5.add_input( op4 )
        wps = cwt.WPS( 'http://localhost:9001/wps', log=True, log_file=os.path.expanduser("~/esgf_api.log") )
        wps.init()
        process = cwt.Process( wps, op5 )
        process.execute( None, d0, [], True, True, "GET" )

executor = DemoWorkflow()
executor.run()