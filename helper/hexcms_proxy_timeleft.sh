#! /bin/bash
#source /osg/current/setup.sh
source /osg/alma8/setup.sh
left=$(voms-proxy-info -timeleft)
echo $left
