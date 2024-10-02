#! /bin/bash
#source /osg/current/setup.sh
#source /osg/alma8/setup.sh
path=$(voms-proxy-info --path)
if [[ -f "$path" ]]; then
  cp $path ./x509up
  exit 0
else
  exit 1
fi
