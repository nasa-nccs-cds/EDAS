#!/usr/bin/env bash
export SCALA_VERSION=2.11
EDAS_JAR=${EDAS_HOME_DIR}/target/scala-${SCALA_VERSION}/edas_${SCALA_VERSION}-${EDAS_VERSION}-SNAPSHOT.jar
SPARK_PRINT_LAUNCH_COMMAND=true
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | edas_cpsed)
echo "Application classpath:  $APP_DEP_CP "
echo "EDAS jar:  $EDAS_JAR "
rm /tmp/${USER}/logs/*
rm -rf $EDAS_CACHE_DIR/block* $EDAS_CACHE_DIR/spark*
$SPARK_HOME/bin/spark-submit --class nasa.nccs.edas.portal.EDASApplication --master local[*] --deploy-mode client --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --driver-class-path "${APP_DEP_CP}" ${EDAS_JAR} 5670 5671 ${EDAS_CACHE_DIR}/edas.properties $*



# cd ${EDAS_HOME_DIR}; python ./python/src/pyedas/initialServiceRequest.py &
# spark-submit --class nasa.nccs.edas.portal.EDASApplication --master local[*] --driver-class-path ${APP_DEP_CP} --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --driver-memory 8g ${EDAS_JAR} 5670 5671 ~/.edas/cache/edas.properties
