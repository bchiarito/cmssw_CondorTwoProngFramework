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
export SCRAM_ARCH=slc7_amd64_gcc820
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
mv $INITIAL_DIR/Cert_*JSON*.txt .
echo ''
echo '&&& Current Directory and Contents: &&&'
pwd
ls -ldh *
echo ''
echo '&&& cmsRun cfg.py &&&'
cmsRun NANOAOD_$4_$5_cfg.py inputFilesFile=cmssw_infiles_$3.dat goodLumis=$6 maxEvents=-1
echo ''
ls -ldh *.root
echo ''
echo '&&& cmsRun completed &&&'
#cd $CMSSW_BASE/src
#git clone -q https://github.com/cms-nanoAOD/nanoAOD-tools.git PhysicsTools/NanoAODTools
#cd PhysicsTools/NanoAODTools
#mv $CMSRUN_DIR/twoprongModule.py ./python/postprocessing/modules/
#mv $CMSRUN_DIR/dropPF.txt .
#mv $CMSRUN_DIR/copy_tree.py .
#echo ''
#echo '&&& Rebuild (scram b) &&&'
#scramv1 b
echo ''
echo '&&& Run NanoAODTools postprocessor &&&'
mv ../../../PhysicsTools/NanoAODTools/test/dropPF.txt .
mv ../../../PhysicsTools/NanoAODTools/test/copy_tree.py .
echo python ../../NanoAODTools/scripts/nano_postproc.py . $CMSRUN_DIR/NanoAOD.root -I PhysicsTools.NanoAODTools.postprocessing.modules.twoprongModule twoprongConstr_$7 --bo dropPF.txt
python ../../NanoAODTools/scripts/nano_postproc.py . $CMSRUN_DIR/NanoAOD.root -I PhysicsTools.NanoAODTools.postprocessing.modules.twoprongModule twoprongConstr_$7,selectionConstr_$8 --bo dropPF.txt
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
FINALFILE=NANOAOD_TwoProng.root
if [ -f "$FINALFILE" ]; then
    :
else 
    echo 'ERROR: No file NANOAOD_TwoProng.root!'
    exit 2
fi

echo '&&& Running Stageout Script with command: &&&'
echo 'python' $2 $3
python $2 $3
exitcode=$?
if [[ exitcode -eq 0 ]] ; then
    :
else
    echo 'ERROR: stageout exited with non-zero exit code!'
    exit 1
fi
echo ''
echo '&&& Finished &&&'
