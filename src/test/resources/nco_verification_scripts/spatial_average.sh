#!/usr/bin/env bash

datafile="/Users/tpmaxwel/Dropbox/Tom/Data/GISS/CMIP5/E2H/r1i1p1/tas_Amon_GISS-E2-H_historical_r1i1p1_185001-190012.nc"

ncwa -O -d lat,5,7 -d lon,25,25 -d time,75,75 -a lat,lon  ${datafile} ~/test/out/spatial_average1.nc
ncdump ~/test/out/spatial_average1.nc