#!/usr/bin/env bash

datafile=/dass/pubrepo/CREATE-IP/data/reanalysis/NASA-GMAO/GEOS-5/MERRA2/6hr/atmos/tas/tas_6hr_reanalysis_MERRA2_1980010100-1980123118.nc

# Time Average
ncwa -O -v tas -d lat,10,10 -d lon,10,50 -a time ${datafile} /tmp/merra2_tas_time_ave.1.nc
ncdump /tmp/merra2_tas_time_ave.1.nc

# Space Average
ncwa -O -v tas -d time,10,30 -a lat,lon ${datafile} /tmp/merra2_tas_space_ave.1.nc
ncdump /tmp/merra2_tas_space_ave.1.nc

# Spatial Max
ncwa -O -v tas -d time,10,30 -a lat,lon -y max ${datafile} /tmp/merra2_tas_time_max.1.nc
ncdump  /tmp/merra2_tas_time_max.1.nc

# Time Min
ncwa -O -v tas -d lat,10,10 -d lon,10,50 -a time -y min ${datafile} /tmp/merra2_tas_time_min.1.nc
ncdump  /tmp/merra2_tas_time_min.1.nc

# Time stdDev
ncks -O -v tas --mk_rec_dmn time  -d lat,5,5 -d lon,5,25 ${datafile} /tmp/merra2_6hr_tas_subset.nc
ncwa -O -v tas -a time /tmp/merra2_6hr_tas_subset.nc /tmp/merra2_6hr_tas_time_ave.nc
ncbo -O -v tas /tmp/merra2_6hr_tas_subset.nc /tmp/merra2_6hr_tas_time_ave.nc /tmp/merra2_6hr_tas_dev.nc
ncra -O -v tas -y rms /tmp/merra2_6hr_tas_dev.nc /tmp/merra2_6hr_tas_stddev.nc
ncdump /tmp/merra2_6hr_tas_stddev.nc

# Anomaly
ncks -O -v tas --mk_rec_dmn time  -d lat,25,25 -d lon,35,35 -d time,10,30 ${datafile} /tmp/merra2_6hr_tas_subset.2.nc
ncwa -O -v tas -a time /tmp/merra2_6hr_tas_subset.2.nc /tmp/merra2_6hr_tas_time_ave.nc
ncbo -O -v tas /tmp/merra2_6hr_tas_subset.2.nc /tmp/merra2_6hr_tas_time_ave.nc /tmp/merra2_6hr_tas_dev.nc
ncdump /tmp/merra2_6hr_tas_dev.nc