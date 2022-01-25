#! /bin/bash
# Usage: ./script.sh -b

UnitTest()
{
  python condor_submit.py one_cmslpc_file.txt . -d unit_test_cmslpc -t -f
  echo ""
  python condor_submit.py one_cmslpc_file.txt /store/user/bchiari1/unit_tests/ -d unit_test_cmslpc -t -f
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
