#! /bin/bash
# Usage: ./script.sh -b

UnitTest()
{
  printf "\n--- Test 1 ---\n\n"
  python condor_submit.py one_cmslpc_file.txt /uscms/home/bchiari1/scratch -d unit_test_cmslpc -t -f
  printf "\n--- Test 2 ---\n\n"
  python condor_submit.py one_cmslpc_file.txt /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -t -f
  printf "\n--- Test 3 ---\n\n"
  python condor_submit.py /DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM -m 1 /uscms/home/bchiari1/scratch -d unit_test_cmslpc -t -f
}
Rebuild()
{
  echo "Got -b option: Rebuild"
  ./cmssw_src_setup.sh
  UnitTest
}
NoRebuild()
{
  echo "No Rebuild"
  UnitTest
}

while getopts ":b" option; do
   case $option in
      b) # Rebuild scratch area
         Rebuild
         exit;;
   esac
done
NoRebuild
