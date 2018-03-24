#!/usr/bin/env bash

rm ${SPARK_HOME}/logs/*
rm -rf ${SPARK_HOME}/work/*
rm -rf ${EDAS_CACHE_DIR}/blockmgr-*
rm -rf ${EDAS_CACHE_DIR}/spark-*
rm /tmp/${USER}/logs/*

cd ${EDAS_HOME_DIR}

sbt "runMain nasa.nccs.edas.portal.SparkCleanup"