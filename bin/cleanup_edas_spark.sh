#!/usr/bin/env bash

rm ${SPARK_HOME}/logs/*
rm -rf ${SPARK_HOME}/work/*

cd ${EDAS_HOME_DIR}

sbt "runMain nasa.nccs.edas.portal.SparkCleanup"