#! /bin/sh
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
#export SCRAM_ARCH=slc7_amd64_gcc820 # note gcc700 will give error when running pfnano code
source $VO_CMS_SW_DIR/cmsset_default.sh
rm -rf prebuild_v1
mkdir prebuild_v1
cd prebuild_v1
scramv1 project CMSSW CMSSW_10_6_19_patch2
cd CMSSW_10_6_19_patch2/src
eval `scramv1 runtime -sh`
git clone https://github.com/bchiarito/cmssw_CustomPFNanoTwoProng-v1.git .
#scram b -j 10
