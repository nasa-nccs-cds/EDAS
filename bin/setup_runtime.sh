#!/usr/bin/env bash
export EDAS_BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export EDAS_HOME_DIR=${EDAS_BIN_DIR}/..
export EDAS_CACHE_DIR=${EDAS_CACHE_DIR:-${HOME}/.edas/cache}
export EDAS_VERSION=1.2
export EDAS_MAX_MEM=${EDAS_MAX_MEM:-32000M}
export CDWPS_HOME_DIR=${CDWPS_HOME_DIR:-${EDAS_HOME_DIR}/../CDWPS}
export CDSHELL_HOME_DIR=${CDSHELL_HOME_DIR:-${EDAS_HOME_DIR}/../EDASClientConsole}
export EDAS_SCALA_DIR=${EDAS_HOME_DIR}/src/main/scala
export EDAS_STAGE_DIR=${EDAS_HOME_DIR}/target/universal/stage
# export CLASSPATH=${EDAS_SCALA_DIR}:${EDAS_CACHE_DIR}:${EDAS_STAGE_DIR}/conf:${EDAS_STAGE_DIR}/lib:${CONDA_PREFIX}/lib:${CLASSPATH}
export UVCDAT_ANONYMOUS_LOG=no
export EDAS_JAVA_ARGS="-J-Xmx$EDAS_MAX_MEM -J-Xms512M -J-Xss1M -J-XX:+CMSClassUnloadingEnabled -J-XX:+UseConcMarkSweepGC"
export WPS_CMD="$CDWPS_HOME_DIR/target/universal/cdwps-1.1-SNAPSHOT/bin/cdwps $EDAS_JAVA_ARGS"
export CDSHELL_CMD="$CDSHELL_HOME_DIR/target/universal/stage/bin/edasclientconsole $EDAS_JAVA_ARGS"
export PATH=${HOME}/.edas/sbin:${EDAS_STAGE_DIR}/bin:${EDAS_BIN_DIR}:${PATH}
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$CONDA_PREFIX/lib
export SBT_OPTS="-Xmx2G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -Xss2M -Duser.timezone=GMT"

alias edas='cd $EDAS_HOME_DIR'
alias cdist='cd $CDWPS_HOME_DIR; sbt dist; cd target/universal/; rm -rf cdwps-*-SNAPSHOT; unzip *.zip; cd ../..; chmod -R a+rwX target; chmod -R a+rX ../CDWPS'
alias pdist='cd $EDAS_HOME_DIR; sbt universal:packageBin; cd target/universal/; rm -rf edas-*-SNAPSHOT; unzip *.zip; cd ../..; chmod -R a+rwX target; chmod -R a+rX ../EDAS'
alias cdwps=$WPS_CMD
alias cdwpsb='nohup $WPS_CMD &'
alias pedas='cd $EDAS_HOME_DIR; git fetch; git pull; sbt publish'
alias cdshd='unset EDAS_SERVER_ADDRESS; unset EDAS_SERVER_PORT; $CDSHELL_CMD'
alias cdshw='export EDAS_SERVER_ADDRESS=localhost; unset EDAS_SERVER_PORT; $CDSHELL_CMD'
alias cdshr='export EDAS_SERVER_ADDRESS=localhost; export EDAS_SERVER_PORT=9001; $CDSHELL_CMD'
alias cdup='cd $EDAS_HOME_DIR; ./bin/update.sh; rm ~/.edas/*.log; python ./python/src/pyedas/shutdown.py'
alias cdupy='cd $EDAS_HOME_DIR; python setup.py install; rm ~/.edas/logs/*.log; rm ~/.edas/*.log; ~/.edas/sbin/shutdown_python_worker.sh'
alias rncml='rm $EDAS_CACHE_DIR/collections/NCML/*'
alias rfrag='rm $EDAS_CACHE_DIR/fragment/*'
alias sclean='cd $EDAS_HOME_DIR; sbt "runMain nasa.nccs.edas.portal.SparkCleanup"'
alias espark='stop-all.sh; cd $EDAS_HOME_DIR; sbt "runMain nasa.nccs.edas.portal.SparkCleanup"'
alias sspark='start-all.sh'

umask 002

