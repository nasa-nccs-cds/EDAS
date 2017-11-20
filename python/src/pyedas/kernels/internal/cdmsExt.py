from pyedas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pyedas.edasArray import cdmsArray, npArray
from cdms2.avariable import AbstractVariable
from cdms2.coord import AbstractCoordinateAxis
import cdms2, time, os, cdutil, genutil
import numpy as np
import numpy.ma as ma

def sa2f( sarray ): return [ float(x) for x in sarray ]
def sa2i( sarray ): return [ int(x) for x in sarray ]

class EnsembleWorkflowKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ensembleStats", "EnsembleWorkflow", "Ensemble Ave/Stdev Workflow as defined by Jerry Potter", handlesInput=True ) )
        self._debug = False

    def executeOperations(self, task, _inputs ):
        """
        :type task: Task
        :type _inputs: dict[str,npArray]
        """
        cdms2.setAutoBounds(2)
        start = time.time()
        mdata = task.metadata;     """:type : dict[str,str] """
        self.logger.info( " Execute REGRID Task with metadata: " + str( task.metadata ) )
        target = str( mdata.get("target","") )
        method = str( mdata.get("method","linear") ).lower()
        res = sa2f( self.getListParm( mdata, "res" ) )
        shape = sa2i( self.getListParm( mdata, "shape" ) )
        plevs = sa2f( self.getListParm( mdata, "plev" ) )
        resultDir = task.metadata.get("resultDir")

        target_input = _inputs.get( target, None )
        if( target_input is None ):
            raise Exception( "Must set the 'target' parameter in EnsembleWorkflowKernel to the id of the input that determines the output grid")

        outgrid = target_input.getGrid()
        arrays = []
        for (id,_input) in _inputs.iteritems():
            dset_address = _input.metadata.get("uri", _input.metadata.get("dataPath") )
            vname = _input.metadata.get("name")
            dset = cdms2.open( dset_address )
            var = dset( vname, genutil.picker( plev=plevs ) ) if( plevs ) else dset( vname );   """:type : AbstractVariable """
            if( id == target ):
                lat=var.getLatitude();           """:type : AbstractCoordinateAxis """
                lon=var.getLongitude();          """:type : AbstractCoordinateAxis """
                plev=var.getLevel();             """:type : AbstractCoordinateAxis """
                t = var.getTime();               """:type : AbstractCoordinateAxis """
                units = var.getattribute("units")
                varname = vname
                arrays.append( var )
            else:
                arrays.append( var.regrid(outgrid) )

        times = t.asComponentTime()
        trange = str(times[0]) + "-" + str(times[-1])
        concatenate = ma.masked_array(arrays)
        average = cdms2.MV2.average(concatenate, axis=0)
        ave = cdms2.createVariable(average, axes=(t, plev, lat, lon))
        ave.units = units
        ave.id = varname
        ave.long_name = 'ensemble average'
        outfile = cdms2.open( resultDir + "/" + varname + '_6hr_reanalysis_ensemble_ave_' + trange + '.nc', 'w')
        outfile.write(ave)
        outfile.close()
        stdv = genutil.statistics.std(concatenate, axis=0)
        stdvave = cdms2.createVariable(stdv, axes=(t, plev, lat, lon))
        stdvave.id = varname + '_stdv'
        stdvave.long_name = 'standard deviation'
        stdvave.units = units

        outfile_std = cdms2.open( resultDir + "/" + varname + '6hr_reanalysis_ensemble_std_' + trange + '.nc', 'w')
        outfile_std.write(stdvave)
        outfile_std.close()
        #newgrid.shape

        end = time.time()
        print "Completed EnsembleWorkflowKernel in " + str( (end - start) / 60. ) + " min "

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting  by default", handlesInput=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        self.logger.info( "Executing AverageKernel, input metadata = " + str(_input.metadata) )
        dset_address = _input.metadata.get("uri", _input.metadata.get("dataPath") )
        vname = _input.metadata.get("name")
        dset = cdms2.open( dset_address )
        selector = _input.getSelector( dset[vname] )
        self.logger.info( "exec *EXT* AverageKernel, selector: " + str( selector ) )
        variable = dset( vname, **selector )
        axes = task.metadata.get("axis","xy")
#        weights = task.metadata.get( "weights", "" ).split(",")
#        if weights == [""]: weights = [ ("generate" if( axis == 'y' ) else "equal") for axis in axes ]
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axes, weights=weights, action=action, returned=returned )
        self.logger.info( "Computed result, input shape = " + str(variable.shape) + ", output shape = " + str(result_var.shape))
        rv = self.createResult( result_var, _input, task )
        self.logger.info( "Result data, shape = " + str(result_var.shape) + ", data = " + np.array_str( rv.array() )  )
        return rv

class ZonalAverageDemo(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("zaDemo", "ZonalAverageDemo", "Zonal average from -90 to 90", handlesInput=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        self.logger.info( "Executing AverageKernel, input metadata = " + str(_input.metadata) )
        dset_address = _input.metadata.get("uri", _input.metadata.get("dataPath") )
        vname = _input.metadata.get("name")
        dset = cdms2.open( dset_address )
        selector = _input.getSelector( dset[vname] )
        self.logger.info( "exec *EXT* AverageKernel, selector: " + str( selector ) )
        variable = dset( vname, **selector )
        axisIndex = variable.getAxisIndex( 'longitude' )

        cdutil.times.setTimeBoundsMonthly(variable)
        djfclimatology = cdutil.times.DJF.climatology(variable)
        zonalAve = cdutil.averager( djfclimatology, axis=axisIndex, weights='equal' )

        return self.createResult( zonalAve, _input, task )
