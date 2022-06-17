#! /bin/bash
source /osg/current/setup.sh
path=$(voms-proxy-info --path)
if [[ -f "$path" ]]; then
  echo $path
  exit 0
fi
