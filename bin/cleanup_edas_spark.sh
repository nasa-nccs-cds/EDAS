#!/usr/bin/env bash

stop-all.sh

rm ${SPARK_HOME}/logs/*
rm -rf ${SPARK_HOME}/work/*
rm -rf ${EDAS_CACHE_DIR}/blockmgr-*
rm -rf ${EDAS_CACHE_DIR}/spark-*
rm /tmp/${USER}/logs/*

cd ${EDAS_HOME_DIR}

pkill -u $USER java

sbt "runMain nasa.nccs.edas.portal.SparkCleanup"