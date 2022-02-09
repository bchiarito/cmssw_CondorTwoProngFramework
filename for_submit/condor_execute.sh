#! /bin/bash
export INITIAL_DIR=$(pwd)
echo "&&& Here there are all the input arguments &&&"
echo $@
echo ''
echo '&&& Current directiory and Contents: &&&'
pwd
ls -ldh *
echo ''

echo '&&& Running input unpacker script with command: &&&'
echo 'python' $1 $3
python $1 $3
echo ''
echo '&&& New contents: &&&'
ls -ldh *
echo ''

echo '&&& Setup CMSSW area &&&'
export HOME=$INITIAL_DIR
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
export SCRAM_ARCH=slc7_amd64_gcc700
source $VO_CMS_SW_DIR/cmsset_default.sh
scramv1 project CMSSW CMSSW_10_6_20
cd CMSSW_10_6_20/src
eval `scramv1 runtime -sh`
mv $INITIAL_DIR/PhysicsTools $CMSSW_BASE/src/
mv $INITIAL_DIR/CommonTools $CMSSW_BASE/src/
echo ''
echo '&&& Building (scram b) &&&'
scramv1 b
echo ''
echo '&&& Setup finished &&&'
echo ''
echo '&&& Current Directory and Contents: &&&'
pwd
ls -ldh *
echo ''
echo '&&& CMSSW_BASE: &&&'
echo $CMSSW_BASE
echo '&&& HOME: &&&'
echo $HOME
echo '&&& ROOTSYS: &&&'
echo $ROOTSYS
echo '&&& ROOT version &&&'
export DISPLAY=localhost:0.0
root -l -q -e "gROOT->GetVersion()"
unset DISPLAY
echo ''

echo '&&& Begin Job Main Payload &&&'
echo ''
echo '&&& cd to PhysicsTools/PFNano/test/ and bring rootfiles from initial dir &&&'
cd PhysicsTools/PFNano/test/
export CMSRUN_DIR=$(pwd)
mv $INITIAL_DIR/cmssw_infiles_$3.dat .
mv $INITIAL_DIR/*.root .
echo ''
echo '&&& Current Directory and Contents: &&&'
pwd
ls -ldh *
echo ''
echo '&&& cmsRun cfg.py inputFilesFile=cmssw_infiles_X.dat &&&'
cmsRun NANOAOD_mc_UL18_cfg.py inputFilesFile=cmssw_infiles_$3.dat goodLumis=$4
echo ''
ls -ldh *.root
echo ''
echo '&&& cmsRun completed, moving back to src/ to checkout NanoAODTools framework &&&'
cd $CMSSW_BASE/src
git clone -q https://github.com/cms-nanoAOD/nanoAOD-tools.git PhysicsTools/NanoAODTools
cd PhysicsTools/NanoAODTools
mv $CMSRUN_DIR/twoprongModule.py ./python/postprocessing/modules/
mv $CMSRUN_DIR/dropPF.txt .
mv $CMSRUN_DIR/copy_tree.py .
echo ''
echo '&&& Rebuild (scram b) &&&'
scramv1 b
echo ''
echo '&&& Run NanoAODTools postprocessor &&&'
python scripts/nano_postproc.py . $CMSRUN_DIR/NanoAOD.root -I PhysicsTools.NanoAODTools.postprocessing.modules.twoprongModule myModuleConstr --bo dropPF.txt
echo ''
echo '&&& Run copy_tree.py &&&'
python copy_tree.py NanoAOD_Skim.root

echo ''
echo '&&& Finished Main Job Payload &&&'
echo ''
echo '&&& Current Directory and Contents: &&&'
pwd
ls -ldh *
echo ''

echo '&&& Moving final rootfile back to initial directory &&&'
mv NANOAOD_TwoProng.root $INITIAL_DIR
cd $INITIAL_DIR
echo ''
echo '&&& Current Directory and Contents: &&&'
pwd
ls -ldh *
echo ''

echo '&&& Running Stageout Script with command: &&&'
echo 'python' $2 $3
python $2 $3
echo ''
echo '&&& Finished &&&'
