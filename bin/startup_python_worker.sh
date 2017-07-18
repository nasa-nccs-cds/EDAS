#!/usr/bin/env bash

source $HOME/.edas/sbin/setup_runtime.sh
source activate edas
python -m pyedas.worker $*

