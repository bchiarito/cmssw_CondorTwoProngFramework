#! /bin/bash
export INITIAL_DIR=$(pwd)
echo "&&& Here there are all the input arguments &&&"
echo $@
echo ''
echo '&&& Current directiory: &&&'
pwd
echo ''
echo '&&& Contents: &&&'
ls -l
echo ''

echo '&&& Running input unpacker script with command: &&&'
echo 'python' $1 $3
python $1 $3
echo ''
echo '&&& Current directiory: &&&'
pwd
echo ''
echo '&&& Contents: &&&'
ls -l
echo ''

echo '&&& Setup CMSSW_10_6_20 Area and move to src/ &&&'
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
export SCRAM_ARCH=slc7_amd64_gcc700
source $VO_CMS_SW_DIR/cmsset_default.sh
scramv1 project CMSSW CMSSW_10_6_20
cd CMSSW_10_6_20/src
eval `scramv1 runtime -sh`
echo ''
echo '&&& Setup finished &&&'
echo ''
echo '&&& CMSSW_BASE: &&&'
echo $CMSSW_BASE
echo ''
echo '&&& Current directiory: &&&'
pwd
echo ''
echo '&&& Contents: &&&'
ls -l
echo ''

### Do main payload here ! ###
touch NANOAOD_TwoProng.root

echo '&&& Finished all steps &&&'
echo ''
echo '&&& Current directiory: &&&'
pwd
echo ''
echo '&&& Contents: &&&'
ls -l

echo ''
echo '&&& Moving result to initial directory &&&'
mv NANOAOD_TwoProng.root $INITIAL_DIR
cd $INITIAL_DIR
echo ''
echo '&&& Current directiory: &&&'
pwd
echo ''
echo '&&& Contents: &&&'
ls -l

echo ''
echo '&&& Running Stageout Script with command &&&'
echo 'python' $2 $3
python $2 $3
echo ''
echo '&&& Finished &&&'
