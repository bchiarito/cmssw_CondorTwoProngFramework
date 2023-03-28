#!/usr/bin/env python3
import os
import sys
import argparse
import time
import socket
# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')
# import condor modules
fix_condor_hexcms_script = 'hexcms_fix_python.sh'
try:
  import classad
  import htcondor
except ImportError as err:
  if site == 'hexcms':
    raise err
  if site == 'cmslpc':
    print('ERROR: Could not import classad or htcondor. Verify that python is default and not from cmssw release (do not cmsenv).')
    raise err

parser = argparse.ArgumentParser(description="copies all output from nano or atto jobs to local storage. matches job directories based on starting with a string parameter")
parser.add_argument('string', help='job directories matched based on starting with this string')
parser.add_argument('dest', default='/uscms/home/bchiari1/nobackup/', help='local storage destination')
args = parser.parse_args()

destination = args.dest
os.system('mkdir -p '+destination)

loc = "."
dirs = []
for d in os.listdir(loc):
  if os.path.isdir(d) and d.startswith(args.string):
    #print(os.path.join(loc, d))
    dirs.append(os.path.join(loc, d))

print(dirs,'\n')

# import job
for i, jobDir in enumerate(dirs):
  sys.path.append(jobDir)
  #print(sys.path)
  import job_info as job
  output_area = job.output
  #print(i, jobDir, output_area)
  del job
  sys.path.pop()
  sys.modules.pop('job_info')
  #print(sys.path)
  if output_area[0:7] == '/store/': output_eos = True
  else: output_eos = False
  #print('')
  if output_eos:
    print('xrdcp -r --nopbar root://cmseos.fnal.gov/'+output_area+' '+destination+''+os.path.basename(jobDir))
    temp = os.getcwd()
    os.chdir(destination)
    os.system('mkdir '+os.path.basename(jobDir))
    os.system('xrdcp -r --nopbar root://cmseos.fnal.gov/'+output_area+' '+destination+''+os.path.basename(jobDir))
    os.chdir(temp)
