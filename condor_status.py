#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import re
import json
import time
from calendar import timegm
import datetime
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

parser = argparse.ArgumentParser(description="")
parser.add_argument("jobDir",help="the condor_submit.py job directory")
parser.add_argument("-v", "--verbose", default=False, action="store_true",help="turn on debug messages")
parser.add_argument("-s", "--summary", default=False, action="store_true",help="do not print one line per job, instead summarize number of jobs with each status type")
parser.add_argument("--onlyFinished", default=False, action="store_true",help="ignore 'running' and 'submitted' job Ids")
parser.add_argument("--notFinished", default=False, action="store_true",help="ignore 'finished' job Ids")
parser.add_argument("--onlyError", default=False, action="store_true",help="ignore 'running', 'submitted', and 'finished, job Ids")
parser.add_argument("--onlyResubmits", default=False, action="store_true",help="only job Ids resubmitted at least once")
parser.add_argument("--group", default=False, action="store_true",help="group according to job Id status, instead of numerical order")
parser.add_argument("--aborted", "-a", default = False, action="store_true", help="print a string of 'aborted' job IDs for condor_resubmit.py")
parser.add_argument("--noOutput", default = False, action="store_true", help="print a string of 'fin w/o output' job IDs for condor_resubmit.py")
args = parser.parse_args()

# constants
json_filename = 'temp.json'

# import job
if args.verbose: print("DEBUG: Import job")
sys.path.append(args.jobDir)
import job_info as job
cluster = job.cluster
procs = range(int(job.queue))
first_proc = int(job.first_proc)
schedd_name = job.schedd_name
output_area = job.output
output_eos = False
output_hex = False
if output_area[0:7] == '/store/':
  if "eos_area" in output_area: output_hex = True
  else: output_eos = True

# get the schedd
if args.verbose: print("DEBUG: Get Schedd")
coll = htcondor.Collector()
schedd_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
for s in schedd_query:
  if str(s["Name"]) == str(schedd_name):
    schedd_ad = s
schedd = htcondor.Schedd(schedd_ad)
if args.verbose: print("DEBUG:", schedd_ad["Name"])

# discover subjob jobNums
subjobs = {}
for proc in procs:
  subjob = {}
  subjobs[proc] = subjob
if args.verbose:
  print("Empty dictionary:", subjobs)

# discover output files
if args.verbose: print("DEBUG: Discover output files")
if output_eos:
  if args.verbose: print("DEBUG: command", 'eos root://cmseos.fnal.gov ls -lh '+output_area)
  output = subprocess.check_output('eos root://cmseos.fnal.gov ls -lh '+output_area, shell=True)
elif not output_hex: # local
  if args.verbose: print("DEBUG: command", 'ls -lh '+output_area)
  output = subprocess.check_output('ls -lh '+output_area, shell=True) 
if output_eos or not output_hex:
  for line in output.decode('utf-8').split('\n'):
    if args.verbose:
      print(len(line.split()))
      for item in line.split():
        print("  ", item)
    if len(line.split()) <= 2: continue
    try:
      l = line.split()
      fi = l[len(l)-1]
      if output_eos: size = l[4]+' '+l[5]
      else:
        temp = l[4]
        size = temp[0:len(temp)-1]+' '+temp[len(temp)-1]
      u = fi.rfind('_')
      d = fi.rfind('.')
      proc = int(fi[u+1:d]) # from outputfile
      proc = proc - first_proc # accounting for tranches
      if proc >= int(job.queue): continue
      if proc < 0: continue
      subjobs[proc]['size'] = size
    except (IndexError, ValueError):
      print("WARNING: got IndexError or ValueError, may want to check output area directly with (eos) ls.")
      continue
if output_hex:
  print("NOTE: Output is to hexcms, script cannot check output directory,\n  will still report job status")

# parse json job report
if args.verbose: print("DEBUG: parse job report file, creating with condor_wait ...")
regex = r"\{[^{}]*?(\{.*?\})?[^{}]*?\}"
wait_command = 'condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+cluster+'.txt'
try:
  wait_output = subprocess.check_output(wait_command, shell=True)
except subprocess.CalledProcessError as e:
  wait_output = e.output
if args.verbose: print("DEBUG: job report file created")
matches = re.finditer(regex, wait_output.decode('utf-8'), re.MULTILINE | re.DOTALL)
for match in matches:
  block = json.loads(match.group(0))
  date = time.strptime(str(block['EventTime']), '%Y-%m-%dT%H:%M:%S')
  t = timegm(date)
  if block['MyType'] == 'SubmitEvent':
    subjobs[int(block['Proc'])]['resubmitted'] = 0
    subjobs[int(block['Proc'])]['start_time'] = date
    subjobs[int(block['Proc'])]['status'] = 'submitted'
  if block['MyType'] == 'ExecuteEvent':
    subjobs[int(block['Proc'])]['status'] = 'running'
    subjobs[int(block['Proc'])]['reason'] = ''
  if block['MyType'] == 'JobReleaseEvent':
    subjobs[int(block['Proc'])]['status'] = 'rereleased'
  if block['MyType'] == 'JobTerminatedEvent':
    subjobs[int(block['Proc'])]['end_time'] = date
    if block['TerminatedNormally']:
      subjobs[int(block['Proc'])]['status'] = 'finished'
    else:
      subjobs[int(block['Proc'])]['status'] = 'failed'
  if block['MyType'] == 'ShadowExceptionEvent':
    subjobs[int(block['Proc'])]['end_time'] = date
    subjobs[int(block['Proc'])]['status'] = 'exception!'
    subjobs[int(block['Proc'])]['reason'] = block['Message']
  if block['MyType'] == 'FileTransferEvent' and block['Type'] == 6:
    subjobs[int(block['Proc'])]['end_time'] = date
    subjobs[int(block['Proc'])]['status'] = 'transferred'
  if block['MyType'] == 'JobAbortedEvent':
    subjobs[int(block['Proc'])]['end_time'] = date
    subjobs[int(block['Proc'])]['reason'] = block['Reason']
    subjobs[int(block['Proc'])]['status'] = 'aborted'
  if block['MyType'] == 'JobHeldEvent':
    subjobs[int(block['Proc'])]['end_time'] = date
    subjobs[int(block['Proc'])]['reason'] = block['HoldReason']    
    subjobs[int(block['Proc'])]['status'] = 'held'
      
if args.verbose: print("DEBUG: Directory so far")
for num in subjobs:
  if args.verbose: print("subjob", num)
  subjob = subjobs[num]
  for item in subjob:
    if args.verbose: print("  ", item, "=", subjob[item])
if args.verbose: print("")

# process resubmits
resubmits = 0
for resubmit_cluster,procs in job.resubmits:
  if args.verbose: print("DEBUG: found resubmit clusterid:", resubmit_cluster)
  regex = r"\{[^{}]*?(\{.*?\})?[^{}]*?\}"
  wait_command = 'condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+resubmit_cluster+'.txt'
  try:
    output = subprocess.check_output(wait_command, shell=True)
  except subprocess.CalledProcessError as e:
    output = e.output
  if args.verbose: print("DEBUG: job report file created")
  resubmits += 1
  matches = re.finditer(regex, output.decode('utf-8'), re.MULTILINE | re.DOTALL)
  for match in matches:
    block = json.loads(match.group(0))
    date = time.strptime(str(block['EventTime']), '%Y-%m-%dT%H:%M:%S')
    t = timegm(date)
    if not 'Proc' in block: continue # skip uninteresting
    if not int(block['Proc']) in procs: continue # skip noop_jobs
    if block['MyType'] == 'SubmitEvent':
      subjobs[int(block['Proc'])]['status'] = 'resubmitted'
      subjobs[int(block['Proc'])]['resubmitted'] += 1
      subjobs[int(block['Proc'])]['start_time'] = date
      subjobs[int(block['Proc'])].pop('end_time', None)
      subjobs[int(block['Proc'])]['reason'] = ''
    if block['MyType'] == 'ExecuteEvent':
      subjobs[int(block['Proc'])]['status'] = 'running'
      subjobs[int(block['Proc'])]['reason'] = ''
    if block['MyType'] == 'JobHeldEvent':
      subjobs[int(block['Proc'])]['end_time'] = date
      subjobs[int(block['Proc'])]['reason'] = block['HoldReason']
      subjobs[int(block['Proc'])]['status'] = 'held'
    if block['MyType'] == 'JobReleaseEvent':
      subjobs[int(block['Proc'])]['status'] = 'released'
    if block['MyType'] == 'JobTerminatedEvent':
      if block['TotalReceivedBytes'] == 0.0: continue
      subjobs[int(block['Proc'])]['end_time'] = date
      if block['TerminatedNormally']: subjobs[int(block['Proc'])]['status'] = 'finished'
      else: subjobs[int(block['Proc'])]['status'] = 'failed'
    if block['MyType'] == 'FileTransferEvent' and block['Type'] == 6:
      subjobs[int(block['Proc'])]['end_time'] = date
      subjobs[int(block['Proc'])]['status'] = 'transferred'
    if block['MyType'] == 'ShadowExceptionEvent':
      subjobs[int(block['Proc'])]['end_time'] = date
      subjobs[int(block['Proc'])]['status'] = 'exception!'
      subjobs[int(block['Proc'])]['reason'] = block['Message']
    if block['MyType'] == 'JobAbortedEvent':
      subjobs[int(block['Proc'])]['end_time'] = date
      subjobs[int(block['Proc'])]['reason'] = block['Reason']
      subjobs[int(block['Proc'])]['status'] = 'aborted'
  # closed file
  if args.verbose: print("DEBUG: Directory so far")
  for num in subjobs:
    if args.verbose: print("subjob", num)
    subjob = subjobs[num]
    for item in subjob:
      if args.verbose: print("  ", item, "=", subjob[item])
  if args.verbose: print("")

# Print Status
print("Results for ClusterId", cluster, "at schedd", schedd_name)
for count, resubmit_cluster in enumerate(job.resubmits):
  print("  Resubmit", count+1, "ClusterId", resubmit_cluster[0])
print("Job output area:", output_area)
if output_eos:
  output = subprocess.check_output('/uscms/home/bchiari1/util_repo/bin/eosdu -h '+output_area, shell=True).decode('utf-8')
  size = output.strip()[:-1]
  suffix = output.strip()[-1]
  print("Total output size ", size, suffix)
if not output_hex: # local
  output = subprocess.check_output('du -hs '+output_area, shell=True).decode('utf-8')
  size = output.split()[0].strip()[:-1]
  suffix = output.split()[0].strip()[-1]
  print("Total output size ", size, suffix)

if not args.summary and not args.aborted and not args.noOutput: print(' {:<5}| {:<15}| {:<7}| {:<18}| {:<12}| {}'.format(
       'Proc', 'Status', 'Resubs', 'Wall Time', 'Output File', 'Msg'
))
if args.summary: summary = {}
lines = []
aborted_jobs = []
noOutput_jobs = []
for jobNum in subjobs:
  subjob = subjobs[jobNum]
  status = subjob.get('status','unsubmitted')
  reason = subjob.get('reason','')
  reason = reason[0:80]
  if status == 'aborted':
    aborted_jobs.append(jobNum)
  if 'start_time' in subjob and 'end_time' in subjob:
    totalTime = str(datetime.timedelta(seconds = timegm(subjob['end_time']) - timegm(subjob['start_time'])))
  elif 'start_time' in subjob:
    totalTime = str(datetime.timedelta(seconds = timegm(time.localtime()) - timegm(subjob['start_time'])))
  elif 'end_time' in subjob:
    totalTime = time.strftime('%m/%d %H:%M:%S', subjob['end_time']) + " (end)"
  else:
    totalTime = ''
  size = subjob.get('size', "")
  if status=='finished' and size=='' and not output_hex:
    status = 'fin w/o output'
    noOutput_jobs.append(jobNum)
  if args.onlyFinished and (status=='submitted' or status=='running' or status=='unsubmitted'): continue
  if args.onlyError and (status=='submitted' or status=='running' or status=='finished' or status=='unsubmitted'): continue
  if args.notFinished and (status=='finished'): continue
  resubs = subjob.get('resubmitted', '')
  if resubs == 0: resubs = ''
  if args.onlyResubmits and resubs == '': continue  
  lines.append(' {:<5}| {:<15}| {:<7}| {:<18}| {:<12}| {:<70.70}'.format(
         str(jobNum), str(status), str(resubs), str(totalTime), str(size), str(reason)
  ))
  if args.summary:
    if status in summary: summary[status] += 1
    else: summary[status] = 1

if not args.summary and not args.aborted and not args.noOutput:
  if args.group:
    def status(line):
      return (line.split('|'))[1]
    lines = sorted(lines, key=status, reverse=True)
  for line in lines:
    print(line)

if args.summary:
  total = 0
  for key in summary:
    total += summary[key]
  print('{:<15} | {}'.format('Status', 'Job Ids ({} total)'.format(total)))
  for status in summary:
    print('{:<15} | {}'.format(str(status), str(summary[status])))

if args.aborted and len(aborted_jobs) != 0:
  x = 0
  while x < len(aborted_jobs):
      if x == len(aborted_jobs) - 1:
          sys.stdout.write(str(aborted_jobs[x]))
          break
      sys.stdout.write(str(aborted_jobs[x]) + ",")
      x += 1
  sys.stdout.write("\n")
elif args.aborted:
  print("No jobs were aborted")

if args.noOutput and len(noOutput_jobs) != 0:
  x = 0
  while x < len(noOutput_jobs):
      if x == len(noOutput_jobs) - 1:
          sys.stdout.write(str(noOutput_jobs[x]))
          break
      sys.stdout.write(str(noOutput_jobs[x]) + ",")
      x += 1
  sys.stdout.write("\n")
elif args.noOutput:
  print("No jobs finished without output")
