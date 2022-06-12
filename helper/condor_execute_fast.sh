#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ""
echo "&&& Here there are all the input arguments &&&"
echo $@
export INITIAL_DIR=$(pwd)
echo ''
echo '&&& Current directiory and Contents: &&&'
pwd
ls -ldh *
echo ''

if [ -f /osg/current/setup.sh ]; then
  echo "&&& Sourcing grid environment for hexcms &&&"
  source /osg/current/setup.sh
  echo ''
fi

echo ''
echo '&&& Finished &&&'
