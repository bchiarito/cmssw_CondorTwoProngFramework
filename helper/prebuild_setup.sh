#! /bin/sh

echo $1 $2

if [[ "$1" == "sl7" && "$2" == "v2" ]]; then

export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
export SCRAM_ARCH=slc7_amd64_gcc820 # note gcc700 will give error when running pfnano code
source ~/.setup_cmssw_old.sh
rm -rf prebuild
mkdir prebuild
cd prebuild
scramv1 project CMSSW CMSSW_10_6_27
cd CMSSW_10_6_27/src
eval `scramv1 runtime -sh`
git clone https://github.com/bchiarito/cmssw_CustomPFNanoTwoProng.git .
#scram b -j 10

elif [[ "$1" == "sl7" && "$2" == "v1" ]]; then

echo "pass"

elif [[ "$1" == "alma8" && "$2" == "v2" ]]; then

#cmssw-el7 "--bind /cms --bind /home --bind /users" -- /uscms/home/bchiari1/work/test_payload.sh
cmssw-el7 "--bind /uscms_data/d1/bchiari1/" -- helper/test_payload.sh

elif [[ "$1" == "alma8" && "$2" == "v1" ]]; then

echo "pass"

fi
