#! /bin/bash
source /osg/current/setup.sh
path=$(voms-proxy-info --path)
if [[ -f "$path" ]]; then
  echo $path
  exit 0
fi
voms-proxy-init -voms cms --valid 168:00
