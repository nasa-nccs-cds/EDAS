from cdms2.avariable import AbstractVariable
from cdms2.coord import AbstractCoordinateAxis
import cdms2, time, os, cdutil, genutil
import numpy as np
import numpy.ma as ma

dset_address = "file:///dass/nobackup/tpmaxwel/.edas/cache/collections/NCML/CIP_MERRA2_mon_tas.ncml"
vname = "tas"
plev = "100000, 97500, 95000, 92500, 90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000, 55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000"

t0 = time.time()
dset = cdms2.open( dset_address )
var = dset( vname, genutil.picker( plev=plev ) );  """:type : AbstractVariable """
t1 = time.time()

print "Completed read test in time {1}, shape = {2}".format( str(t1-t0), str(var.shape) )
