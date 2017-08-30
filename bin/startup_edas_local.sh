#!/usr/bin/env bash

EDAS_JAR=${EDAS_HOME_DIR}/target/scala-2.10/edas_2.10-1.2.2-SNAPSHOT.jar
CONDA_LIB=${CONDA_PREFIX}/lib
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -e "s/ /:/g" ):${CONDA_LIB}
cd ${EDAS_HOME_DIR}; python ./python/src/pyedas/initialServiceRequest.py &
spark-submit --class nasa.nccs.edas.portal.EDASApplication --master local[*] --driver-class-path ${APP_DEP_CP} --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --driver-memory 8g ${EDAS_JAR} 5670 5671 ~/.edas/cache/edas.properties
