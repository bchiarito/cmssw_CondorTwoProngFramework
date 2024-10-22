#! /bin/sh

if [[ "$1" == "v2" ]]; then
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
export SCRAM_ARCH=slc7_amd64_gcc820 # note gcc700 will give error when running pfnano code
rm -rf prebuild
mkdir prebuild
cd prebuild
scramv1 project CMSSW CMSSW_10_6_27
cd CMSSW_10_6_27/src
eval `scramv1 runtime -sh`
git clone -b $3 https://github.com/bchiarito/cmssw_CustomPFNanoTwoProng.git .
if [[ "$2" == "True" ]]; then
  export TAG=$(git describe --tags --long)
  scram b -j 10
  cd ../..
  tar --exclude=".git" --exclude="*.root" -zcf CMSSW_10_6_27__${TAG}.tgz CMSSW_10_6_27
  rm -rf CMSSW_10_6_27
else
  exit 0
fi
fi

if [[ "$1" == "v1" ]]; then

#! /bin/sh
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
rm -rf prebuild_v1
mkdir prebuild_v1
cd prebuild_v1
scramv1 project CMSSW CMSSW_10_6_19_patch2
cd CMSSW_10_6_19_patch2/src
eval `scramv1 runtime -sh`
git clone -b $3 https://github.com/bchiarito/cmssw_CustomPFNanoTwoProng-v1.git .
#scram b -j 10
exit 0
fi
