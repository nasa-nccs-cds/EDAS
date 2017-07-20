#!/usr/bin/env bash
EDAS_JAR=${EDAS_HOME_DIR}/target/scala-2.10/edas_2.10-1.2.2-SNAPSHOT.jar
SPARK_PRINT_LAUNCH_COMMAND=true 
APP_DEP_JARS=$(find ~/.ivy2 -name "*.jar" )
APP_DEP_CP=$(echo $APP_DEP_JARS | sed -r 's/[ \n\r]+/:/g')
echo "Application classpath:  $APP_DEP_CP "
$CDH_HOME/bin/spark-submit --class nasa.nccs.edas.portal.EDASApplication --master yarn --deploy-mode client --conf "spark.executor.extraClassPath=${APP_DEP_CP}" --conf "spark.driver.extraClassPath=${APP_DEP_CP}" --driver-memory 8g ${EDAS_JAR} bind 5670 5671 /home/tpmaxwel/.edas/cache/edas.properties.yarn

