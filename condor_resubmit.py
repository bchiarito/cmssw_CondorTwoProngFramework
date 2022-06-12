#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import re
import json
import time
import socket
from calendar import timegm
import datetime
import socket

# constants
submit_filename = 'submit_file.jdl'

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')

# import condor modules
try:
  import classad
  import htcondor
except ImportError as err:
  if site == 'hexcms':
    raise err
  if site == 'cmslpc':
    print('ERROR: Could not import classad or htcondor. Verify that python is default and not from cmssw release (do not cmsenv).')
    raise err

parser = argparse.ArgumentParser(description="")
parser.add_argument("jobDir",help="the condor_submit.py job directory")
parser.add_argument("procs",help="Process numbers to resubmit, e.g.: 1-3,5,6,7")
parser.add_argument("--batch",help="JobBatchName parameter, displays when using condor_q -batch")
parser.add_argument("-v", "--verbose", default=False, action="store_true",help="turn on debug messages")
args = parser.parse_args()

# import job
if args.verbose: print("DEBUG: Import job")
sys.path.append(args.jobDir)
import job_info as job
cluster = job.cluster
procs = range(int(job.queue))
first_proc = int(job.first_proc)
schedd_name = job.schedd_name
output_area = job.output
if output_area[0:7] == '/store/': output_eos = True
else: output_eos = False

# get the schedd
if args.verbose: print("DEBUG: Get Schedd")
coll = htcondor.Collector()
if site == 'cmslpc':
  schedd_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
  for s in schedd_query:
    if str(s["Name"]) == str(schedd_name):
      schedd_ad = s
  schedd = htcondor.Schedd(schedd_ad)
if site == 'hexcms':
  schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
  schedd = htcondor.Schedd(schedd_ad)
if args.verbose: print("DEBUG:", schedd_ad["Name"])

# make list of procs to resubmit
procs = []
for a, b in re.findall(r'(\d+)-?(\d*)', args.procs):
  procs.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
procs_string = ''
for p in procs:
  procs_string += str(p)+','
procs_string = procs_string[:-1]

# create new submit jdl
batchName = "resub_for_"+job.cluster if args.batch is None else args.batch
with open(args.jobDir+'/'+submit_filename) as f:
  submit_string = f.read()
  submit_string += "\nJobBatchName = " + batchName
  submit_string += '\nnoop_job = !stringListMember("$(Process)","'+procs_string+'")'
  submit_string += '\nTEMP = $(Process) + ' + str(first_proc)
  submit_string += '\nGLOBAL_PROC = $INT(TEMP)'

# make submit object
sub = htcondor.Submit(submit_string)

# move/delete old output
for proc in procs:
  base = 'NANOAOD_TwoProng_'
  jobid = str(proc + first_proc)
  file_to_remove = base + jobid + '.root'
  if output_eos:
    rm_command = 'eos root://cmseos.fnal.gov rm ' + output_area + '/' + file_to_remove + ' 2> /dev/null'
  else: # local
    rm_command = 'rm -f ' + output_area + '/' + file_to_remove + ' 2> /dev/null'
  if args.verbose: print("DEBUG: remove command ", rm_command)
  try:
    output = subprocess.check_output(rm_command, shell=True)
  except Exception:
    pass

# submit the job
if args.verbose: print("DEBUG: Submitting Jobs")
with schedd.transaction() as txn:
  cluster_id = sub.queue(txn, count=int(job.queue))
  print("ClusterId: ", cluster_id)

# update job_info.py
with open(args.jobDir+'/job_info.py', 'a') as f:
  f.write("resubmits.append(('"+str(cluster_id)+"',["+procs_string+"]))\n")
