#! /bin/sh
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
export SCRAM_ARCH=slc7_amd64_gcc700
source $VO_CMS_SW_DIR/cmsset_default.sh
rm -rf prebuild
mkdir prebuild
cd prebuild
scramv1 project CMSSW CMSSW_10_6_20
cd CMSSW_10_6_20/src
eval `scramv1 runtime -sh`
#git config --global user.name 'placeholder placeholder'
#git config --global user.email 'placeholder@mail.com'
#git config --global user.github placeholder
git cms-rebase-topic andrzejnovak:614nosort
git clone https://github.com/cms-jet/PFNano.git PhysicsTools/PFNano
git clone -b temp_forcondor https://github.com/bchiarito/cmssw_TwoProngCustomNano.git
source cmssw_TwoProngCustomNano/setup.sh
