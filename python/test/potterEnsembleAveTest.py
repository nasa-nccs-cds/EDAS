import cdms2,cdtime,genutil
import time, genutil
import numpy.ma as ma
start=time.time()
month =['1980010100-1980013118',
'1980020100-1980022918',
'1980010100-1980013118',
'1980020100-1980022918',
'1980030100-1980033118',
'1980040100-1980043018',
'1980050100-1980053118',
'1980060100-1980063018',
'1980070100-1980073118',
'1980080100-1980083118',
'1980090100-1980093018',
'1980100100-1980103118',
'1980110100-1980113018',
'1980120100-1980123118']
#get the desired resolution from JRA-55 (1.25x1.25)
f1=cdms2.open('/att/dataportal01/CREATE-IP/reanalysis/JMA/JRA-55/6hr/atmos/ta/ta_6hr_reanalysis_JRA-55_1980010100-1980013118.nc')


ta2=f1('ta',genutil.picker(plev=(100000, 97500, 95000, 92500, 90000,
87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000, 55000,
50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)))
ta2=ta2(time=('1980-01-00','cob'))
outfixlev=cdms2.open('/att/dmznsd/nobackup01/projects/ESGF/glpotter/CFSR/ensemble/level_test.nc','w')

ta2.id='ta'
outfixlev.write(ta2)
outfixlev.close()
f1=cdms2.open('/att/dmznsd/nobackup01/projects/ESGF/glpotter/CFSR/ensemble/level_test.nc')
ta1=f1('ta')
outgrid=ta1.getGrid()
lat=f1.getAxis('lat')
lon=f1.getAxis('lon')
plev=f1.getAxis('plev')


# extract levels and regrid for each month - no need to regrid JRA-55

for ii in range(0,len(month)):
f=cdms2.open('/att/dataportal01/CREATE-IP/reanalysis/NOAA-NCEP/CFSR/6hr/atmos/ta/ta_6hr_reanalysis_CFSR_'+month[ii]+'.nc')
         ta=f('ta',genutil.picker(plev=(100000, 97500, 95000, 92500,
90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000,
55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)))
         ta1=ta.regrid(outgrid)
         t=ta1.getTime()

f=cdms2.open('/att/dataportal01/CREATE-IP/reanalysis/ECMWF/IFS-Cy31r2/6hr/atmos/ta/ta_6hr_reanalysis_IFS-Cy31r2_'+month[ii]+'.nc')
         ta=f('ta',genutil.picker(plev=(100000, 97500, 95000, 92500,
90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000,
55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)))
         ta2=ta.regrid(outgrid)

f=cdms2.open('/att/dataportal01/CREATE-IP/reanalysis/NASA-GMAO/GEOS-5/MERRA2/6hr/atmos/ta/ta_6hr_reanalysis_MERRA2_'+month[ii]+'.nc')
         ta=f('ta',genutil.picker(plev=(100000, 97500, 95000, 92500,
90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000,
55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)))
         ta3=ta.regrid(outgrid)

f=cdms2.open('/att/dataportal01/CREATE-IP/reanalysis/NASA-GMAO/GEOS-5/MERRA/6hr/atmos/ta/ta_6hr_reanalysis_MERRA_'+month[ii]+'.nc')
         ta=f('ta',genutil.picker(plev=(100000, 97500, 95000, 92500,
90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000,
55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)))
         ta4=ta.regrid(outgrid)


f=cdms2.open('/att/dataportal01/CREATE-IP/reanalysis/JMA/JRA-55/6hr/atmos/ta/ta_6hr_reanalysis_JRA-55_'+month[ii]+'.nc')
         ta5=f('ta',genutil.picker(plev=(100000, 97500, 95000, 92500,
90000, 87500, 85000, 82500, 80000, 77500, 75000, 70000, 65000, 60000,
55000, 50000, 45000, 40000, 35000, 30000, 25000, 20000, 15000, 10000)))


# combine arrays making ensemble member a dimension
         arrays=[ta1,ta2,ta3,ta4,ta5]
         concatenate=ma.masked_array(arrays)
         average=cdms2.MV2.average(concatenate,axis=0)
         ave=cdms2.createVariable(average,axes=(t,plev,lat,lon))
         ave.units='K'
         ave.id='ta'
         ave.long_name='ensemble average'
outfile=cdms2.open('/att/dmznsd/nobackup01/projects/ESGF/glpotter/ensemble/ta_6hr_reanalysis_ensemble_ave_'+month[ii]+'.nc','w')
         outfile.write(ave)
         outfile.close()
         stdv=genutil.statistics.std(concatenate,axis=0)
         stdvave=cdms2.createVariable(stdv,axes=(t,plev,lat,lon))
         stdvave.id='ta_stdv'
         stdvave.long_name='standard deviation'
         stdvave.units='K'

outfile_std=cdms2.open('/att/dmznsd/nobackup01/projects/ESGF/glpotter/ensemble/ta_6hr_reanalysis_ensemble_std_'+month[ii]+'.nc','w')
         outfile_std.write(stdvave)
         outfile_std.close()
         #newgrid.shape

end=time.time()
print (end-start)/60.

