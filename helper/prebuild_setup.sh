#! /bin/sh

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

elif [[ "$1" == "sl7" && "$2" == "v1" ]]; then
echo "pass"

elif [[ "$1" == "alma8" && "$2" == "v2" ]]; then
if [[ "$3" == "hexcms" ]]; then
export PATH="/cvmfs/oasis.opensciencegrid.org/mis/apptainer/1.2.5/bin:$PATH"
cmssw-el7 "--bind /condor --bind /osg --bind /cms --bind /home --bind /users" -- helper/test_payload.sh
elif [[ "$3" == "cmslpc" ]]; then
cmssw-el7 "--bind /uscms_data/d1/$USER/" -- helper/test_payload.sh
fi

elif [[ "$1" == "alma8" && "$2" == "v1" ]]; then
if [[ "$3" == "hexcms" ]]; then
export PATH="/cvmfs/oasis.opensciencegrid.org/mis/apptainer/1.2.5/bin:$PATH"
cmssw-el7 "--bind /condor --bind /osg --bind /cms --bind /home --bind /users" -- helper/test_payload_v1.sh
elif [[ "$3" == "cmslpc" ]]; then
cmssw-el7 "--bind /uscms_data/d1/$USER/" -- helper/test_payload_v1.sh
fi

fi
