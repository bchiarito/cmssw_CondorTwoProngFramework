#! /bin/sh
#source /home/joey/alma8_setups/setup_inside_cmssw-el7_apptainer.sh
#cmsrel CMSSW_10_6_27
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
export SCRAM_ARCH=slc7_amd64_gcc820 # note gcc700 will give error when running pfnano code
#source /home/joey/alma8_setups/setup_inside_cmssw-el7_apptainer.sh
#source ~/.setup_cmssw_old.sh
rm -rf prebuild
mkdir prebuild
cd prebuild
scramv1 project CMSSW CMSSW_10_6_27
cd CMSSW_10_6_27/src
eval `scramv1 runtime -sh`
git clone https://github.com/bchiarito/cmssw_CustomPFNanoTwoProng.git .
#scram b -j 10
