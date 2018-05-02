import numpy as np
import numpy.ma as ma
import time, traceback, logging, struct, sys
from messageParser import mParse
IO_DType = np.dtype( np.float32 ).newbyteorder('>')
from abc import ABCMeta, abstractmethod

def getFillValue( array ):
    try:    return array.get_fill_value()
    except: return sys.float_info.max

def a2s( array ): return ', '.join(map(str, array))

def shape( elem ):
    try: return a2s(elem.shape)
    except: "UNDEF"

class CDArray:
    __metaclass__ = ABCMeta

    def __init__(self, _id, _origin, _shape, _metadata ):
        self.logger = logging.getLogger()
        self.id = _id
        self.origin = _origin
        self.shape = _shape
        self.metadata = _metadata
        self.roi = self.parseRoi()
        self.logger.debug("Created Array: {0}".format(self.id))
        self.logger.debug(" >> Array Metadata: {0}".format(self.metadata))
        self.logger.debug(" >> Array Shape: [{0}]".format(', '.join(map(str, self.shape))))
        self.logger.debug(" >> Array Origin: [{0}]".format(', '.join(map(str, self.origin))))
        self.logger.debug(" >> Array ROI: [{0}]".format(', '.join(map(str, self.roi.items()))))

    @classmethod
    @abstractmethod
    def createResult(cls, task, input, result_array ): raise Exception( "Executing abstract method createResult in CDArray")

    def uid(self): return self.id.split('-')[0]

    def getGridBounds(self):
        gridBnds = self.metadata.get("gridbnds")
        if ( gridBnds is None ): return None
        else:
            bndVals = [ float(grdBnd) for grdBnd in gridBnds.split(",") ]
            return ( (bndVals[0],bndVals[1]), (bndVals[2],bndVals[3]) )


    @classmethod
    @abstractmethod
    def createInput(cls, header, data): raise Exception( "Executing abstract method createInput in CDArray")

    @abstractmethod
    def getVariable( self, gridFile = None ): pass

    @abstractmethod
    def array(self): pass

    @abstractmethod
    def getGrid(self): pass

    @abstractmethod
    def subsetAxes(self, dimensions, gridfile, origin, shape, latBounds, lonBounds ): pass

    @abstractmethod
    def toBytes( self, dtype ): pass

    def getAxisSection( self, axis ): return None if self.roi is None else self.roi.get( axis.lower(), None )

    def parseRoi(self):
        roiSpec = self.metadata.get("roi")
        roiMap = {}
        if (roiSpec is None):
            self.logger.info(" >>>->> Empty ROI spec {0} in CDArray".format( self.id ) )
            axisIds = ['t','z','y','x'] if( len(self.shape) == 4 ) else ['t','y','x']
            for axisIndex in range( len(axisIds) ):
                roiMap[axisIds[axisIndex]] = ( self.origin[axisIndex], self.origin[axisIndex] + self.shape[axisIndex] )
        else:
            self.logger.info(" >>>->> Parsing ROI spec: {0}".format( roiSpec ) )
            for roiTok in roiSpec.split('+'):
                axisToks = roiTok.split(',')
                roiMap[ axisToks[0].lower() ] = ( int(axisToks[1]),  int(axisToks[2]) + 1 )
        return roiMap

class npArray(CDArray):

    @classmethod
    def createResult(cls, task, input, result_array ):
        """  :rtype: npArray """
        return npArray( task.rId, input.origin, result_array.shape, dict( input.metadata, **task.metadata ), result_array, input.undef )

    @classmethod
    def createAuxResult( cls, id, metadata, input, result_array ):
        """  :rtype: npArray """
        return npArray( id, input.origin, result_array.shape, metadata, result_array, input.undef )

    def compress( self, condition, axis ):
        """  :rtype: npArray """
        result_array = self.array.compress( condition, self.array, axis )
        return npArray( self._id, self._origin, result_array.shape, self._metadata, result_array, self._undef )

    def toBytes( self, dtype ):
        return self.array.astype(dtype).tobytes() + np.array([self.undef]).astype(dtype).tobytes() # bytearray(struct.pack("f", self.undef))

    @classmethod
    def createInput(self, header, data):
        """  :rtype: npArray """
        logger = logging.getLogger()
        logger.info(" ***->> Creating Input, header = {0}".format( header ) )
        header_toks = header.split('|')
        id = header_toks[1]
        origin = mParse.s2it(header_toks[2])
        shape = mParse.s2it(header_toks[3])
        metadata = mParse.s2m(header_toks[4])
        if data:
            try:
                logger.info(" ***>> Creating Input, id = {0}, data size = {1}, shape = {2}".format( id, len(data), str(shape) ) )
                raw_data = np.frombuffer( data, dtype=IO_DType ).astype(np.float32)
                undef_value = raw_data[-1]
                logger.info(" *** buffer len = {0}, undef = {1}, head = {2}".format( str(len(raw_data)), str(undef_value), str(raw_data[0]) ) )
                data_array = ma.masked_invalid( raw_data[0:-1].reshape(shape) )
                logger.info( " Creating input array: " + id + ", shape = " + str(shape) + ", header = " + header + " metadata = " + str(metadata) )
                nparray =  ma.masked_equal(data_array,undef_value) if ( undef_value != 1.0 ) else data_array
            except Exception as err:
                logger.info( " !!!!! Error creating input array: " + str(err) )
                nparray = None
                undef_value = float('inf')
        else:
            nparray = None
            undef_value = float('inf')
        return npArray( id, origin, shape, metadata, nparray, undef_value )

    @classmethod
    def createInput1(self, header, data):
        """  :rtype: npArray """
        logger = logging.getLogger()
        logger.info(" ***->> Creating Input, header = {0}".format( header ) )
        header_toks = header.split('|')
        id = header_toks[1]
        origin = mParse.s2it(header_toks[2])
        shape = mParse.s2it(header_toks[3])
        metadata = mParse.s2m(header_toks[4])
        if data:
            logger.info(" *** Creating Input, id = {0}, data size = {1}, shape = {2}".format( id, len(data), str(shape) ) )
            raw_data = np.frombuffer( data, dtype=IO_DType ).astype(np.float32)
            undef_value = raw_data[-1]
            logger.info(" *** buffer len = {0}, undef = {1}".format( str(len(raw_data)), str(undef_value) ) )
            data_array = ma.masked_invalid( raw_data[0:-1].reshape(shape) )
            nparray =  ma.masked_equal(data_array,undef_value) if ( undef_value != 1.0 ) else data_array
        else:
            nparray = None
            undef_value = float('inf')
        return npArray( id, origin, shape, metadata, nparray, undef_value )

    def __init__(self, _id, _origin, _shape, _metadata, _ndarray, _undef ):
        super(npArray, self).__init__(_id,_origin,_shape,_metadata)
        self.gridFile = self.metadata["gridfile"]
        self.name = self.metadata.get("name","")
        self.collection = self.metadata.get("collection","")
        self.dimensions = self.metadata["dimensions"].split(",")    # Ordered list of coordinate axis names
        self.array = _ndarray
        self.undef = _undef
        self.variable = None
        self.logger.info(" *** Creating NP data array, nbytes = " + str(self.nbytes()) + ", undef = " + str(self.undef) )

    def getSelector(self, variable, **args):
        kargs = {}
        for idim in range( variable.rank() ):
            axis = variable.getAxis(idim)
            start = self.origin[idim]
            end = start + self.shape[idim]
            interval = [ start, end ]
            kargs[axis.id] = slice(*interval)
        return kargs

    def nbytes(self): return self.array.nbytes if not (self.array is None) else 0
    def array(self): return self.array

    def getGrid1(self):
        import cdms2
        gridfile = cdms2.open(self.gridFile)
        baseGrid = gridfile.grids.values()[0]
        gridBnds = self.getGridBounds()
        if ( gridBnds is None ):  return baseGrid
        else:
            variable = self.getVariable()
            (lataxis, lonaxis) = (variable.getLatitude(), variable.getLongitude())
            (latInterval, lonInterval) = (lataxis.mapInterval( gridBnds[0] ), lonaxis.mapInterval( gridBnds[1] ))
            return baseGrid.subGrid( latInterval, lonInterval )

    def getGrid(self, _gridFilePath = None ):
        import cdms2
        gridFilePath = self.gridFile if (_gridFilePath == None) else _gridFilePath
        gridfile = cdms2.open( gridFilePath )
        baseGrid = gridfile.grids.values()[0]
        (latInterval, lonInterval) = ( self.getAxisSection('y'), self.getAxisSection('x') )
        if ( (latInterval is None) or (lonInterval is None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def getVariable( self, _gridFilePath = None ):
        """  :rtype: cdms2.tvariable.TransientVariable """
        import cdms2
        if( self.variable is None ):
            t0 = time.time()
            gridFilePath = self.gridFile if (_gridFilePath==None) else _gridFilePath
            gridfile = cdms2.open( gridFilePath )
            var = gridfile[self.name]
            baseGrid = gridfile.grids.values()[0]
            (latInterval, lonInterval) = (self.getAxisSection('y'), self.getAxisSection('x'))
            grid = baseGrid if ((latInterval is None) or (lonInterval is None)) else baseGrid.subGrid(latInterval, lonInterval)
            partition_axes = self.subsetAxes( self.dimensions, gridfile, self.origin, self.shape, latInterval, lonInterval )

            self.logger.info( "Creating Variable {0}, gridfile = {1}, data shape = [ {2} ], self.shape = [ {3} ], grid shape = [ {4} ], roi = {5}, baseGrid shape = [ {6} ], latInterval = {7}, lonInterval = {8}".format(
                self.name, gridFilePath, a2s(self.array.shape), a2s(self.shape), a2s(grid.shape), str(self.roi), a2s(baseGrid.shape), str(latInterval), str(lonInterval) ) )
            self.variable = cdms2.createVariable( self.array, typecode=None, copy=0, savespace=0, mask=None, fill_value=var.getMissing(), missing_value=var.getMissing(),
                                            grid=grid, axes=partition_axes, attributes=self.metadata, id=self.collection + "-" + self.name)
            self.variable.createattribute("gridfile", self.gridFile)
            self.variable.createattribute("origin", mParse.ia2s(self.origin))
            for paxis in partition_axes:
                self.logger.info( " Partition axis: {0}, startVal: {1}".format( paxis.axis, paxis[0] ) )
            self.logger.info( " --Grid-> Lat startVal: " + str(grid.getLatitude()[0]) )
            for raxis in self.variable.getAxisList():
                self.logger.info( " Regrid axis: {0}, startVal: {1}".format( raxis.axis, raxis[0] ) )
            t1 = time.time()
            self.logger.info(" >> Created CDMS Variable: {0} ({1}) in time {2}, gridFile = {3}".format(self.variable.id, self.name, (t1 - t0), self.gridFile))
        return self.variable

    def subsetAxes( self, dimensions, gridfile, origin, shape, latBounds, lonBounds ):
        subAxes = []
        try:
            for index in range( len(dimensions) ):
                length = shape[index]
                dim = dimensions[index]
                axis = gridfile.axes.get(dim)
                start = lonBounds[0] if( axis.axis == 'X' ) else latBounds[0] if( axis.axis == 'Y' ) else origin[index]
                subAxis = axis.subAxis( start, start + length )
                subAxes.append( subAxis )
                self.logger.info( " >> Axis {0}: {1}, length: {2}, start: {3}, startVal: {4} ".format( axis.axis, dim, length, start, subAxis[0] ) )
        except Exception as err:
            self.logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            raise err
        return subAxes


class cdmsArray(CDArray):

    @classmethod
    def createModifiedResult(cls, id, origin, metadata, cdVariable ):
        return cdmsArray( id, origin, cdVariable.shape, metadata, cdVariable )

    @classmethod
    def createResult(cls, task, input, cdVariable ):
        return cdmsArray( task.rId, input.origin, cdVariable.shape, dict( input.metadata, **task.metadata ), cdVariable )

    @classmethod
    def getName(cls, variable ):
        try:        return variable.name_in_file;
        except:     return variable.id;


    @classmethod
    def createInput( cls, cdVariable ):
        id = cdVariable.id
        origin = cdVariable.attributes.get("origin")
        shape = cdVariable.shape
        metadata = cdVariable.attributes
        return cdmsArray( id, origin, shape, metadata, cdVariable )

    def array(self):
        return self.variable.data

    def toBytes( self, dtype ):
        return self.variable.data.astype(dtype).tobytes() + np.array([ self.variable.getMissing() ]).astype(dtype).tobytes() # bytearray( struct.pack("f", self.variable.getMissing()))

    def __init__(self, _id, _origin, _shape, _metadata, cdVariable ):
        super(cdmsArray, self).__init__( _id, _origin, _shape, _metadata )
        self.logger.info(" *** Creating input cdms array, size = " + str( cdVariable.size ) )
        self.name = cdmsArray.getName(cdVariable)
        self.grid = cdVariable.getGrid()
        self.dimensions = self.metadata["dimensions"].split(",")
        self.variable = cdVariable

    def getVariable( self, gridFile= None ): return self.variable

    def getGrid(self):
        baseGrid = self.variable.getGrid()
        (latInterval, lonInterval) = ( self.getAxisSection('y'), self.getAxisSection('x') )
        if ( (latInterval is None) or (lonInterval is None)  ):  return baseGrid
        else: return baseGrid.subGrid( latInterval, lonInterval )

    def getGrid1(self):
        gridBnds = self.getGridBounds()
        if ( gridBnds is None ):  return self.variable.getGrid()
        else:
            (lataxis, lonaxis) = (self.variable.getLatitude(), self.variable.getLongitude())
            (latInterval, lonInterval) = (lataxis.mapInterval( gridBnds[0] ), lonaxis.mapInterval( gridBnds[1] ))
            self.logger.info( " latInterval {0} --- lonInterval {1} ".format( str(latInterval), str(lonInterval) ) )
            return self.variable.getGrid().subGrid( latInterval, lonInterval )

    def subsetAxes( self, dimensions, gridfile, origin, shape, latBounds, lonBounds ):
        subAxes = []
        try:
            for index in range( len(dimensions) ):
                length = shape[index]
                dim = dimensions[index]
                axis = gridfile.axes.get(dim)
                start = lonBounds[0] if (axis.axis == 'X') else latBounds[0] if (axis.axis == 'Y') else origin[index]
                subAxes.append( axis.subAxis( start, start + length ) )
                self.logger.info( " >> Axis: {0}, length: {1} ".format( dim, length ) )
        except Exception as err:
            self.logger.info( "\n-------------------------------\nError subsetting Axes: {0}\n{1}-------------------------------\n".format(err, traceback.format_exc() ) )
            raise err
        return subAxes


