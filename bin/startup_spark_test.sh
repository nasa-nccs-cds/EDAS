#!/usr/bin/env bash
SCALA_VERSION=2.11
EDAS_JAR=${EDAS_HOME_DIR}/target/scala-${SCALA_VERSION}/edas_${SCALA_VERSION}-${EDAS_VERSION}-SNAPSHOT.jar
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -r 's/[ \n\r]+/:/g')
$SPARK_HOME/bin/spark-submit --class nasa.nccs.edas.portal.TestApplication --master spark://cldralogin101:7077 --deploy-mode client --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --conf "spark.driver.extraClassPath=${APP_DEP_CP}" --driver-memory 10G ${EDAS_JAR} 5670 5671 ${EDAS_CACHE_DIR}/edas.properties
