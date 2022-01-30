import os
import sys
import argparse

parser = argparse.ArgumentParser(description="")
parser.add_argument("jobDir",help="the condor_submit.py job directory")
parser.add_argument("dest",help="local absolute path for job rootfiles")
parser.add_argument("-a", "--add", action="store_true",help="hadd all rootfiles together")
args = parser.parse_args()

# constants
hadd_filename = "summed.root"

# create specified local directory if it doesnt exist
os.system("mkdir -p " + args.dest)

# determine where output is located
location = "cmslpc"

# copy files to specified local directory
sys.path.append(args.jobDir)
import stageout as job
if location is "cmslpc":
  os.system("xrdcp -r "+job.redirector+job.output_location+" "+args.dest)
  if not len(os.listdir(args.dest)) == 1: raise Exception("something went wrong.")
  di = os.listdir(args.dest)[0]
  d = args.dest+di
  print d
  print di
  os.system("mv "+d+"/* "+args.dest)
  os.system("rmdir "+d)
if location is "local":
  os.system("cp + "+job.output_location+"/* "+args.dest)

# hadd into one file, if option given
if args.add:
  os.system("hadd "+args.dest+"/"+hadd_filename+" `ls -1 "+args.dest+"/* | grep .root`")

# remove individual files, if did hadd
if args.add:
  os.system("rm `ls -1 "+args.dest+"/* | grep -v "+hadd_filename+" | grep .root`")
