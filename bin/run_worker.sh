#!/usr/bin/env bash
source ~/.bash_profile
EDAS_WORKER=${EDAS_HOME_DIR}/python/src/pyedas/worker.py
echo "Running Python worker: "
echo ${EDAS_WORKER}
python ${EDAS_WORKER} $*



