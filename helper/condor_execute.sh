#! /bin/bash
echo ">>> Starting job on" `date`
echo ">>> Running on: `uname -a`"
echo ">>> System software: `cat /etc/redhat-release`"
echo ""
echo "&&& Here there are all the input arguments &&&"
echo "&&& unpacker stageout proc mc/data year lumi twoprongSB photonSB selection &&&"
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

if [ -f x509up ]; then
  export X509_USER_PROXY=$INITIAL_DIR/x509up
  voms-proxy-info -all
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
export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch
source $VO_CMS_SW_DIR/cmsset_default.sh
echo '&&& now cmsrel &&&'

if [[ ${11} == "v2" ]]; then
  export SCRAM_ARCH=slc7_amd64_gcc820
  scramv1 project CMSSW CMSSW_10_6_27
  cd CMSSW_10_6_27/src
  eval `scramv1 runtime -sh`
  mv $INITIAL_DIR/PhysicsTools $CMSSW_BASE/src/
  mv $INITIAL_DIR/EgammaAnalysis $CMSSW_BASE/src/
  mv $INITIAL_DIR/EgammaPostRecoTools $CMSSW_BASE/src/
  mv $INITIAL_DIR/RecoEgamma $CMSSW_BASE/src/
fi

if [[ ${11} == "v1" ]]; then
  export SCRAM_ARCH=slc7_amd64_gcc700
  scramv1 project CMSSW CMSSW_10_6_19_patch2
  cd CMSSW_10_6_19_patch2/src
  eval `scramv1 runtime -sh`
  mv $INITIAL_DIR/PhysicsTools $CMSSW_BASE/src/
  mv $INITIAL_DIR/CommonTools $CMSSW_BASE/src/
fi

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
echo $INITIAL_DIR
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
mv $INITIAL_DIR/infiles_$3.dat .
mv $INITIAL_DIR/infiles_$3.txt .
mv $INITIAL_DIR/*.root .
mv $INITIAL_DIR/Cert_*JSON*.txt .
echo ''
echo '&&& Current Directory and Contents: &&&'
pwd
ls -ldh *
echo ''
echo '&&& Get number of rootfile events &&&'
which jq
if [ -f $INITIAL_DIR/jq-linux64 ]; then
  shopt -s expand_aliases
  alias jq="$INITIAL_DIR/jq-linux64"
fi
ROOTFILE_EVENTS=$(edmFileUtil -j $(echo file:$(ls *.root)) | jq '.[0].events')
echo 'Got:'
echo ${ROOTFILE_EVENTS}
echo ''
echo '&&& cmsRun cfg.py &&&'

if [ -f infiles_$3.dat ]; then
  if [ $4 == "data" ]; then
    cmsRun -j report.xml NANOAOD_$4_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.dat goodLumis=$6
  elif [ $4 == "mc" ]; then
    cmsRun -j report.xml NANOAOD_$4_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.dat goodLumis=$6
  elif [ $4 == "sigRes" ]; then
    cmsRun -j report.xml NANOAOD_mc_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.dat goodLumis=$6 photonsf=True
  elif [ $4 == "sigNonRes" ]; then
    cmsRun -j report.xml NANOAOD_mc_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.dat goodLumis=$6
  else
    echo '&&& ERROR! Could not determine data/mc/signal !!! &&&'
  fi
fi
if [ -f infiles_$3.txt ]; then
  if [ $4 == "data" ]; then
    cmsRun -j report.xml NANOAOD_$4_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.txt goodLumis=$6
  elif [ $4 == "mc" ]; then
    cmsRun -j report.xml NANOAOD_$4_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.txt goodLumis=$6
  elif [ $4 == "sigRes" ]; then
    cmsRun -j report.xml NANOAOD_mc_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.txt goodLumis=$6 photonsf=True
  elif [ $4 == "sigNonRes" ]; then
    cmsRun -j report.xml NANOAOD_mc_$5_cfg.py maxEvents=${ROOTFILE_EVENTS} inputFilesFile=infiles_$3.txt goodLumis=$6
  else
    echo '&&& ERROR! Could not determine data/mc/signal !!! &&&'
  fi
fi

echo ''
ls -ldh *.root
echo ''
echo '&&& cmsRun completed &&&'
echo ''

echo '&&& Run NanoAODTools postprocessor &&&'
# run twoprong and photon modules
python ../../NanoAODTools/scripts/nano_postproc.py . $CMSRUN_DIR/NanoAOD.root -I PhysicsTools.NanoAODTools.postprocessing.modules.main twoprongConstr_$7,photonConstr_$8
# run modified twoprong module
if [[ "${10}" == "extratrack" && "$7" == "addLoose" ]]; then
  python ../../NanoAODTools/scripts/nano_postproc.py . NanoAOD_Skim.root -I PhysicsTools.NanoAODTools.postprocessing.modules.main twoprongConstr_optionalTrack_addLoose
  mv NanoAOD_Skim_Skim.root NanoAOD_Skim.root
elif [[ "${10}" == "extratrack"  && "$7" == "default" ]]; then
  python ../../NanoAODTools/scripts/nano_postproc.py . NanoAOD_Skim.root -I PhysicsTools.NanoAODTools.postprocessing.modules.main twoprongConstr_optionalTrack
  mv NanoAOD_Skim_Skim.root NanoAOD_Skim.root
fi
# run signal modules
if [[ $4 == "sigRes" ]]; then
  python ../../NanoAODTools/scripts/nano_postproc.py . NanoAOD_Skim.root -I PhysicsTools.NanoAODTools.postprocessing.modules.main genpartConstr_res
  mv NanoAOD_Skim_Skim.root NanoAOD_Skim.root
fi
if [[ $4 == "sigNonRes" ]]; then
  python ../../NanoAODTools/scripts/nano_postproc.py . NanoAOD_Skim.root -I PhysicsTools.NanoAODTools.postprocessing.modules.main genpartConstr_nonres
  mv NanoAOD_Skim_Skim.root NanoAOD_Skim.root
fi
# run selection module and drop branches
mv ../../../PhysicsTools/NanoAODTools/test/dropPF.txt .
python ../../NanoAODTools/scripts/nano_postproc.py . NanoAOD_Skim.root -I PhysicsTools.NanoAODTools.postprocessing.modules.main selectionConstr_$9 --bo dropPF.txt
mv NanoAOD_Skim_Skim.root NanoAOD_Skim.root

echo ''
echo '&&& Run copy_tree.py &&&'
mv ../../../PhysicsTools/NanoAODTools/test/copy_tree.py .
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
mv report.xml $INITIAL_DIR
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
