#! /bin/bash
# Usage: ./script.sh -b

Rebuild()
{
  echo "Got -b option: Rebuild"
  python condor_submit.py one_hexcms_file.txt . -d unit_test_hexcms -b -t -f
}
NoRebuild()
{
  echo "No Rebuild"
  python condor_submit.py one_hexcms_file.txt . -d unit_test_hexcms -t -f
}

while getopts ":b" option; do
   case $option in
      b) # display Help
         Rebuild
         exit;;
   esac
done
NoRebuild
