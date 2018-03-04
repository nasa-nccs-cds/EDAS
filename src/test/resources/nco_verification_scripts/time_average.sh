#!/usr/bin/env bash

# datafile="/Users/tpmaxwel/Dropbox/Tom/Data/MERRA/MERRA2/6hr/MERRA2_200.inst6_3d_ana_Np.20000101.nc4"
#ncwa -O -v tas -d lat,20,23 -d lon,30,33 -a time ${datafile_agg} ~/test/out/time_sum_agg.nc
#ncwa -O -v tas -a time,lat,lon ${datafile_agg} ~/test/out/spacetime_ave_agg.nc
#ncdump ~/test/out/spacetime_ave_agg.nc


datafile_agg="/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1_agg/tas_Amon_GISS-E2-H_historical_r1i1p1_agg.nc"
ncwa -O -v tas -d lat,5,5 -d lon,5,10 -a time -y total ${datafile_agg} ~/test/out/minval.nc
ncdump ~/test/out/minval.nc




