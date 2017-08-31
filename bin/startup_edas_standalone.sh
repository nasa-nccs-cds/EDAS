#!/usr/bin/env bash
EDAS_JAR=${EDAS_HOME_DIR}/target/scala-2.10/edas_2.10-1.2.2-SNAPSHOT.jar
SPARK_PRINT_LAUNCH_COMMAND=true 
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -r 's/[ \n\r]+/:/g')
echo "Application classpath:  $APP_DEP_CP "
rm ~/.edas/logs/edas-* ~/.edas/logs/python-*
rm -rf $EDAS_CACHE_DIR/block* $EDAS_CACHE_DIR/spark*
cd ${EDAS_HOME_DIR}; python ./python/src/pyedas/initialServiceRequest.py &
$SPARK_HOME/bin/spark-submit --class nasa.nccs.edas.portal.EDASApplication --master spark://cldralogin101:7077 --deploy-mode client --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --conf "spark.driver.extraClassPath=${APP_DEP_CP}" --driver-memory 10G ${EDAS_JAR} 5670 5671 ${EDAS_CACHE_DIR}/edas.properties

