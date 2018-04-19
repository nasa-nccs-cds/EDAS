#!/usr/bin/env bash
SCALA_VERSION=2.11
EDAS_JAR=${EDAS_HOME_DIR}/target/scala-${SCALA_VERSION}/edas_${SCALA_VERSION}-${EDAS_VERSION}-SNAPSHOT.jar
CONDA_LIB=${CONDA_PREFIX}/lib
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | edas_cpsed)
cd ${EDAS_HOME_DIR}; python ./python/src/pyedas/initialServiceRequest.py &
spark-submit --class nasa.nccs.edas.portal.EDASApplication --master local[*] --driver-class-path ${APP_DEP_CP} --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --driver-memory 8g ${EDAS_JAR} 5670 5671 ~/.edas/cache/edas.properties
