#! /bin/bash
# Usage: ./script.sh -b

# 3 minute real submit test
# $ python condor_submit.py /store/user/lpcrutgers/sthayil/pseudoaxions/ttPhiPS_M-1000/miniAOD_33.root /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -f

test_file_eos=/store/user/lpcrutgers/sthayil/pseudoaxions/ttPhiPS_M-1000/miniAOD_33.root
test_file_local=/uscms/home/bchiari1/nobackup/miniaod/849A52BD-E632-4849-AEAE-33C988932E2F.root

UnitTest()
{
  printf "\n--- Test 1 ---\n\n"
  python condor_submit.py $test_file_local --input_local /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -t -f
  printf "\n--- Test 2 ---\n\n"
  python condor_submit.py $test_file_eos /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -t -f
  printf "\n--- Test 3 ---\n\n"
  python condor_submit.py /DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM -m 1 /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -t -f
  printf "\n--- Test 4 ---\n\n"
  python condor_submit.py /DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM -m 1 /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -t -f --useLFN
}
Rebuild()
{
  echo "Got -b option: Rebuild"
  ./for_submit/cmssw_src_setup.sh
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
