#! /bin/bash
# Usage: ./script.sh -b

UnitTest()
{
  printf "\n--- Test 1 ---\n\n"
  python condor_submit.py one_hexcms_file.txt /home/chiarito/unit_test_hexcms/ -d unit_test_hexcms -t -f
  printf "\n--- Test 2 ---\n\n"
  python condor_submit.py /DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM -m 1 /cms/chiarito/eos/for_condor/unit_test_hexcms/ -d unit_test_cmslpc -t -f
  printf "\n--- Test 3 ---\n\n"
  python condor_submit.py /DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/RunIISummer20UL18MiniAODv2-106X_upgrade2018_realistic_v16_L1v1-v2/MINIAODSIM -m 1 /cms/chiarito/eos/for_condor/unit_test_hexcms/ -d unit_test_cmslpc -t -f --useLFN
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
