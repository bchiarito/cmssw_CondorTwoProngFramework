#! /bin/sh

if [[ "$1" == "sl7" ]]; then

  echo 'sl7 got' $2 $4 $5

elif [[ "$1" == "alma8" ]]; then

  if [[ "$3" == "hexcms" ]]; then
  export PATH="/cvmfs/oasis.opensciencegrid.org/mis/apptainer/1.2.5/bin:$PATH"
  cmssw-el7 "--bind /condor --bind /osg --bind /cms --bind /home --bind /users" -- helper/prebuild.sh $2 $4 $5
  elif [[ "$3" == "cmslpc" ]]; then
  cmssw-el7 "--bind /uscms_data/d1/$USER/" -- helper/prebuild.sh $2 $4 $5
  elif [[ "$3" == "lxplus" ]]; then
  cmssw-el7 -- helper/prebuild.sh $2 $4 $5
  fi

fi
