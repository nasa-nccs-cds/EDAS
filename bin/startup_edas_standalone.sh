#!/usr/bin/env bash
SCALA_VERSION=2.11
EDAS_JAR=${EDAS_HOME_DIR}/target/scala-${SCALA_VERSION}/edas_${SCALA_VERSION}-${EDAS_VERSION}-SNAPSHOT.jar
SPARK_PRINT_LAUNCH_COMMAND=true 
# IVY_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo ${EDAS_HOME_DIR}/lib | sed -r 's/[ \n\r]+/:/g')
echo "Application classpath:  $APP_DEP_CP "
echo "EDAS jar:  $EDAS_JAR "
rm /tmp/${USER}/logs/*
rm -rf $EDAS_CACHE_DIR/block* $EDAS_CACHE_DIR/spark*
$SPARK_HOME/bin/spark-submit --class nasa.nccs.edas.portal.EDASApplication --master spark://cldralogin101:7077 --deploy-mode client --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --conf "spark.driver.extraClassPath=${APP_DEP_CP}" --driver-memory 32G ${EDAS_JAR} 5670 5671 ${EDAS_CACHE_DIR}/edas.properties

