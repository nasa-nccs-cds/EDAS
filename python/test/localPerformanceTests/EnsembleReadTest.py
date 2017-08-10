from cdms2.avariable import AbstractVariable
from cdms2.fvariable import FileVariable
from cdms2.coord import AbstractCoordinateAxis
import cdms2, time, os, cdutil, genutil
import numpy as np
import numpy.ma as ma

dataportal_data_path = '/att/dataportal01/CREATE-IP/reanalysis'
dass_data_path= '/dass/pubrepo/CREATE-IP/data/reanalysis'
data_path = dass_data_path

target_grid_dset = cdms2.open( data_path + '/JMA/JRA-55/6hr/atmos/ta/ta_6hr_reanalysis_JRA-55_1958010100-1958013118.nc' )
target_grid_var = target_grid_dset['ta'];  """:type : FileVariable """

dset_address = data_path + "/NASA-GMAO/GEOS-5/MERRA2/mon/atmos/tas/tas_Amon_reanalysis_MERRA2_198001-201412.nc"
vname = "tas"
levs = (100000, 97500, 95000, 92500, 90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000, 55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)

t0 = time.time()
dset = cdms2.open( dset_address )
var0 = dset[vname]
var1 = dset( vname, genutil.picker( level=levs ) );  """:type : AbstractVariable """
# newvar = var.regrid( target_grid_var.getGrid() )
t1 = time.time()

print "Completed test in time {0}, shape0 = {1}, shape1 = {2}".format( str(t1-t0), str( var0.shape ), str( var1.shape ) )
