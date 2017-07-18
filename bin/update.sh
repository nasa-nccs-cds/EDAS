#!/usr/bin/env bash

git fetch
git pull

cd ${EDAS_HOME_DIR}
sbt publish
sbt stage
python setup.py install
