#!/usr/bin/env bash
source ~/.bash_profile
EDAS_SHUTDOWN=${EDAS_HOME_DIR}/python/src/pyedas/shutdownServer.py
python ${EDAS_SHUTDOWN} $*

