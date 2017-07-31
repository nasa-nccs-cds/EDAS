from pyedas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pyedas.edasArray import cdmsArray
import cdms2, time, os, cdutil
from pyedas.messageParser import mParse
import numpy as np

def sa2f( sarray ): return [ float(x) for x in sarray ]
def sa2i( sarray ): return [ int(x) for x in sarray ]

class RegridKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT", parallelize=True ) )
        self._debug = False

    def getGrid(self, gridFilePath, latInterval = None, lonInterval = None ):
        import cdms2
        gridfile = cdms2.open(gridFilePath)
        baseGrid = gridfile.grids.values()[0]
        if ( (latInterval == None) or (lonInterval == None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def getListParm(self, metadata, id, default="" ):
        """  :rtype: list[int] """
        svals = str(metadata.get(id,default)).lower();  """:type : str """
        return svals.split(",") if svals else []



    def executeOperations(self, task, _inputs):
        """
        :type task: Task
        :type _inputs: dict[str,npArray]
        """
        cdms2.setAutoBounds(2)
        t0 = time.time()
        mdata = task.metadata;     """:type : dict[str,str] """
        self.logger.info( " Execute REGRID Task with metadata: " + str( task.metadata ) )
        gridType = str( mdata.get("grid","uniform") ).lower()
        target = str( mdata.get("target","") )
        gridSpec = str( mdata.get("spec","") )
        method = str( mdata.get("method","linear") ).lower()
        res = sa2f( self.getListParm( mdata, "res" ) )
        shape = sa2i( self.getListParm( mdata, "shape" ) )
        toGrid = None
        if( target ):
            grid_input = _inputs.get( target, None )
            if not grid_input: raise Exception( "Can't find grid variable uid: " + target + ", variable uids = " + str( _inputs.keys() ) )
            toGrid = grid_input.getGrid()
        else :
            if( gridSpec ):
                toGrid = self.getGrid( gridSpec )
            else:
                if( "gaussian" in gridType ):
                    toGrid = cdms2.createGaussianGrid( shape[0] )
                elif( "uniform" in gridType ):
                    origin = sa2f( self.getListParm( mdata, "origin", "0,-90" ) )
                    if( shape ):
                        if( not res ): res = [ (360.0-origin[0])/shape[0], (90.0-origin[1])/shape[1] ]
                    else:
                        if( not res ):  raise Exception( "Must define either 'shape' or 'res' parameter in regrid kernel")
                        shape = [ int(round((360.0-origin[0])/res[0])), int(round((90.0-origin[1])/res[1])) ]
                    toGrid = cdms2.createUniformGrid( origin[0], shape[0], res[0], origin[1], shape[1], res[1] )

        results = []
        for input_id in task.inputs:
            _input = _inputs.get( input_id.split('-')[0] )
            variable = _input.getVariable()
            ingrid = _input.getGrid()
            inlatBounds, inlonBounds = ingrid.getBounds()
            self.logger.info( " >> in LAT Bounds shape: " + str(inlatBounds.shape) )
            self.logger.info( " >> in LON Bounds shape: " + str(inlonBounds.shape) )
            outlatBounds, outlonBounds = toGrid.getBounds()
            self.logger.info( " >> out LAT Bounds shape: " + str(outlatBounds.shape) )
            self.logger.info( " >> out LON Bounds shape: " + str(outlonBounds.shape) )
            if( not ingrid == toGrid ):
                self.logger.info( " Regridding Variable {0} using grid {1} ".format( variable.id, str(toGrid) ) )
                if self._debug:
                    self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) ) )
                    self.logger.info( " >> Input Variable Shape: {0}, Grid Shape: {1} ".format( str(variable.shape), str([len(ingrid.getLatitude()),len(ingrid.getLongitude())] )))

                result_var = variable.regrid( toGrid, regridTool="esmf", regridMethod=method )
                self.logger.info( " >> Gridded Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) ) )
                results.append( self.createResult( result_var, _input, task ) )
        t1 = time.time()
        self.logger.info(" @RRR@ Completed regrid operation for input variables: {0} in time {1}".format( str(_inputs.keys), (t1 - t0)))
        return results

class AverageKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("ave", "Average", "Averages the inputs using UVCDAT with area weighting by default", parallelize=True ) )
        self._debug = False

    def executeOperation(self, task, _input):
        variable = _input.getVariable()
        axis = task.metadata.get("axis","xy")
        #        weights = task.metadata.get( "weights", "" ).split(",")
        #        if weights == [""]: weights = [ ("generate" if( axis == 'y' ) else "equal") for axis in axes ]
        weights = task.metadata.get("weights","generate").split(",")
        if( len(weights) == 1 ): weights = weights[0]
        action = task.metadata.get("action","average")
        returned = 0
        result_var = cdutil.averager( variable, axis=axis, weights=weights, action=action, returned=returned )
        rv = self.createResult( result_var, _input, task )
        return rv
