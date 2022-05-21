#! /bin/bash
source /osg/current/setup.sh
left=$(voms-proxy-info -timeleft)
echo $left
