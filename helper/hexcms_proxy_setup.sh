#! /bin/bash
#source /osg/current/setup.sh
source /osg/alma8/setup.sh
path=$(voms-proxy-info --path)
if [[ -f "$path" ]]; then
  echo $path
  exit 0
fi
