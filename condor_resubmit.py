#!/usr/bin/env python
import os
import sys
import argparse
import subprocess
import re
import json
import time
from calendar import timegm
import datetime
import classad
import htcondor

parser = argparse.ArgumentParser(description="")
parser.add_argument("jobDir",help="the condor_submit.py job directory")
parser.add_argument("procs",help="Process numbers to resubmit, e.g.: 1-3,5,6,7")
parser.add_argument("--batch",help="JobBatchName parameter, displays when using condor_q -batch")
parser.add_argument("-v", "--verbose", default=False, action="store_true",help="turn on debug messages")
args = parser.parse_args()

# constants
json_filename = 'temp.json'
submit_filename = 'submit_file.jdl'

# import job
if args.verbose: print "DEBUG: Import job"
sys.path.append(args.jobDir)
import job_info as job
cluster = job.cluster
procs = range(int(job.queue))
schedd_name = job.schedd_name
output_area = job.output
if output_area[0:7] == '/store/': output_eos = True
else: output_eos = False

# get the schedd
if args.verbose: print "DEBUG: Get Schedd"
coll = htcondor.Collector()
schedd_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
for s in schedd_query:
  if str(s["Name"]) == str(schedd_name):
    schedd_ad = s
schedd = htcondor.Schedd(schedd_ad)
if args.verbose: print "DEBUG:", schedd

# make list of procs to resubmit
procs = []
for a, b in re.findall(r'(\d+)-?(\d*)', args.procs):
  procs.extend(range(int(a), int(a)+1 if b=='' else int(b)+1))
procs_string = ''
for p in procs:
 procs_string += str(p)+','
procs_string = procs_string[:-1]
#print procs
#print procs_string

# create new submit jdl
batchName = "resub_for_"+job.cluster if args.batch is None else args.batch
with open(args.jobDir+'/'+submit_filename) as f:
  submit_string = f.read()
  #submit_string = submit_string.replace('log_'+job.cluster+'.txt', 'log_'+args.proc+'_'+cluster+'.txt')
  submit_string += "\nJobBatchName = " + batchName
  submit_string += '\nnoop_job = !stringListMember("$(Process)","'+procs_string+'")'
  #print submit_string

# make submit object
sub = htcondor.Submit(submit_string)

# check
#i = raw_input()
#if i == 'q': sys.exit()

# move/delete old output
for proc in procs:
  base = 'NANOAOD_TwoProng_'
  jobid = str(proc)
  file_to_remove = base + jobid + '.root'
  if output_eos:
    rm_command = 'eos root://cmseos.fnal.gov rm ' + output_area + '/' + file_to_remove + ' 2> /dev/null'
  else: # local
    rm_command = 'rm -f ' + output_area + '/' + file_to_remove + ' 2> /dev/null'
  if args.verbose: print "DEBUG: remove command ", rm_command
  try:
    output = subprocess.check_output(rm_command, shell=True)
  except Exception:
    pass

# submit the job
if args.verbose: print "DEBUG: Submitting Jobs"
with schedd.transaction() as txn:
  cluster_id = sub.queue(txn, count=int(job.queue))
  print "ClusterId: ", cluster_id

# update job_info.py
with open(args.jobDir+'/job_info.py', 'a') as f:
  f.write("\nresubmits.append('"+str(cluster_id)+"')\n")
