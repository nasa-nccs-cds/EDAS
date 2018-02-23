from pyedas.kernels.Kernel import CDMSKernel, Kernel, KernelSpec
from pyedas.edasArray import cdmsArray, npArray
import cdms2, time, os, cdutil
from pyedas.messageParser import mParse
import numpy as np
import regrid2

def sa2f( sarray ):
    try: return [ float(x) for x in sarray ]
    except: return None

def sa2i( sarray ):
    try: return [ int(x) for x in sarray ]
    except: return None

class RegridKernel(CDMSKernel):

    def __init__( self ):
        Kernel.__init__( self, KernelSpec("regrid", "Regridder", "Regrids the inputs using UVCDAT", parallelize=True, visibility="public" ) )
        self._debug = True

    def getGrid( self, gridFile, latInterval=None, lonInterval=None ):
        import cdms2
        gridfile = cdms2.open(gridFile)
        baseGrid = gridfile.grids.values()[0]
        if ( (latInterval is None) and (lonInterval is None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def getAxisBounds(self, gridSection):
        subGridSpec = gridSection.split(",")[-2:]
        subGridIndices = [map(int, x.split(':')) for x in subGridSpec]
        return ( [subGridIndices[0][0],subGridIndices[0][1]+1], [subGridIndices[1][0],subGridIndices[1][1]+1]  )

    def getOutGrid(self, mdata, inputs, ingrid ):
        #        log_file.write( "\n Execute REGRID Task with metadata: " + str( task.metadata ) + "\n" )
        gridType = str( mdata.get("grid","") ).lower()
        target = str( mdata.get("target","") )
        gridSpec = str( mdata.get("gridSpec","") )
        res = sa2f( self.getListParm( mdata, "res" ) )
        shape = sa2i( self.getListParm( mdata, "shape" ) )
        gridSection = str( mdata.get('gridSection',"") )
        inlatBounds, inlonBounds = ingrid.getBounds()
        self.logger.info("\n Execute REGRID -> " + gridType + ", grid section: '" + str(gridSection) + "' with metadata: " + str(mdata) + "\n")
        self.logger.info(" >> in LAT Bounds shape: " + str(inlatBounds.shape) + ", values: " + str(inlatBounds))
        self.logger.info(" >> in LON Bounds shape: " + str(inlonBounds.shape) + ", values: " + str(inlonBounds))
        toGrid = None
        if ("gaussian" in gridType):
            toGrid = cdms2.createGaussianGrid(shape[0])
            self.logger.info("createGaussianGrid, shape = " + str(toGrid.shape) )
            # if (gridSection):
            #     (bounds0, bounds1) = self.getAxisBounds(gridSection)
            #     toGrid = toGrid.subGrid(bounds0, bounds1)
        elif ("uniform" in gridType):
            origin = sa2f(self.getListParm(mdata, "origin", "0,-90"))
            if (shape):
                if (not res): res = [(90.0 - origin[0]) / shape[0], (360.0 - origin[1]) / shape[1]]
            else:
                if (not res):  raise Exception("Must define either 'shape' or 'res' parameter in regrid kernel")
                shape = [int(round((90.0 - origin[0]) / res[0])), int(round((360.0 - origin[1]) / res[1]))]
            toGrid = cdms2.createUniformGrid(origin[0], shape[0], res[0], origin[1], shape[1], res[1])
            outlatBounds, outlonBounds = toGrid.getBounds()
            outlatBounds0, outlonBounds0 = outlatBounds[0], outlonBounds[0]
            inlatBounds0, inlonBounds0 = inlatBounds[0], inlonBounds[0]
            new_origin = list(origin)
            if( ( outlatBounds0[0] < inlatBounds0[0] ) and ( outlatBounds0[1] > inlatBounds0[0] ) ): origin[0] = origin[0] + (inlatBounds0[0]-outlatBounds0[0])
            if( ( outlonBounds0[0] < inlonBounds0[0] ) and ( outlonBounds0[1] > inlonBounds0[0] ) ): origin[1] = origin[1] + (inlonBounds0[0]-outlonBounds0[0])
            if( cmp(new_origin,origin) ):
                self.logger.info("Re-create uniform Grid, new origin = " + str(new_origin) + " old bounds, lat = " + str( outlatBounds0 ) + ", lon = " + str( outlonBounds0 ))
                toGrid = cdms2.createUniformGrid(new_origin[0], shape[0], res[0], new_origin[1], shape[1], res[1])
            self.logger.info("createUniformGrid")
        elif( target ):
            grid_input = inputs.get( target, None )
            if not grid_input: raise Exception( "Can't find grid variable uid: " + target + ", variable uids = " + str( inputs.keys() ) )
            self.logger.info("create Grid from target")
            toGrid = grid_input.getGrid()
        elif( gridSpec ):
            toGrid = self.getGrid( gridSpec )
            self.logger.info("create Grid from gridSpec")
            if( gridSection ):
                ( bounds0, bounds1 ) = self.getAxisBounds( gridSection )
                toGrid = toGrid.subGrid( bounds0, bounds1 )
        else:
            raise Exception( "Unable to determine target grid type in Regrid operation")

        outlatBounds, outlonBounds = toGrid.getBounds()
        self.logger.info(" >> out LAT Bounds shape: " + str(outlatBounds.shape) + ", values: " + str(outlatBounds))
        self.logger.info(" >> out LON Bounds shape: " + str(outlonBounds.shape) + ", values: " + str(outlonBounds))
        return toGrid

    def executeOperations(self, task, _inputs):
        """
        :type task: Task
        :type _inputs: dict[str,npArray]
        """
#        log_file = open("/tmp/edasadm/logs/debug_log_file.txt","w")
        results = []
        if( len(_inputs) == 0 ):
            self.logger.info( "No inputs to operation; returning empty result" )
        else:
            cdms2.setAutoBounds(2)
            t0 = time.time()
            mdata = task.metadata;     """:type : dict[str,str] """
            regridTool = str(mdata.get("regridTool", "esmf"))
            method = str( mdata.get("method","linear") ).lower()

            for input_id in task.inputs:
                vid = input_id.split('-')[0]
                _input = _inputs.get( vid );    """ :type : npArray """
                if( _input is None ):
                    raise Exception(" Can't find variable id {0} ({1}) in inputs {2} ".format( vid, input_id, str( _inputs.keys() ) ))
                else:
                    self.logger.info( "Getting input for variable {0}, name: {1}, collection: {2}, gridFile: {3}".format( vid, _input.name, _input.collection, _input.gridFile ) )
                    variable = _input.getVariable()
                    ingrid = _input.getGrid()
                    self.logger.info(" >>  in variable grid shape: " + str(variable.getGrid().shape))
                    toGrid = self.getOutGrid( mdata, _inputs, ingrid )
                    if( not ingrid == toGrid ):
                        self.logger.info( " Regridding Variable {0} using grid {1} ".format( variable.id, toGrid.getType() ) )
                        if self._debug:
                            self.logger.info( " >> Input Data Sample: [ {0} ]".format( ', '.join(  [ str( variable.data.flat[i] ) for i in range(20,90) ] ) ) )
                            self.logger.info( " >> Input Variable Shape: {0}, Grid Shape: {1}, Regrid Method: {2}, Grid Type: {3} ".format( str(variable.shape), str([len(ingrid.getLatitude()),len(ingrid.getLongitude())] ), method, toGrid.getType() ))

                        result_var = variable.regrid(toGrid, regridTool=regridTool, regridMethod=method)
                        self.logger.info( " >> Gridded Data Sample: [ {0} ]".format( ', '.join(  [ str( result_var.data.flat[i] ) for i in range(20,90) ] ) ) )
                        results.append( self.createResult( result_var, _input, task ) )
            t1 = time.time()
            self.logger.info(" @RRR@ Completed regrid operation for input variables: {0} in time {1}".format( str( _inputs.keys() ), (t1 - t0)))
    #        log_file.close()
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
