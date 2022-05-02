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

# import condor modules
fix_condor_hexcms_script = 'hexcms_fix_python.sh'
try:
  import classad
  import htcondor
except ImportError as err:
  if site == 'hexcms':
    raise SystemExit('ERROR: On hexcms, please source this file before running: ' + fix_condor_hexcms_script)
  if site == 'cmslpc':
    print 'ERROR: Could not import classad or htcondor. Verify that python is default and not from cmssw release (do not cmsenv).'
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
args = parser.parse_args()

# constants
json_filename = 'temp.json'

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

# discover subjob jobNums
subjobs = {}
for proc in procs:
  subjob = {}
  subjobs[proc] = subjob

# discover output files
if args.verbose: print "DEBUG: Discover output files"
if output_eos:
  if args.verbose: print "DEBUG: command", 'eos root://cmseos.fnal.gov ls -lh '+output_area
  output = subprocess.check_output('eos root://cmseos.fnal.gov ls -lh '+output_area, shell=True)
else: # local
  if args.verbose: print "DEBUG: command", 'ls -lh '+output_area
  output = subprocess.check_output('ls -lh '+output_area, shell=True) 
for line in output.split('\n'):
  #print len(line.split())
  #for item in line.split():
  #  print "  ", item
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
    proc = int(fi[u+1:d])
    subjobs[proc]['size'] = size
  except (IndexError, ValueError):
    print "WARNING: got IndexError or ValueError, may want to check output area directly with (eos) ls."
    continue

# parse json job report
if args.verbose: print "DEBUG: parse job report file, creating with condor_wait ..."
#regex = r"\{(.*?)\}"
regex = r"\{[^{}]*?(\{.*?\})?[^{}]*?\}"
#os.system('condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+cluster+'.txt > '+json_filename)
wait_command = 'condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+cluster+'.txt'
try:
  wait_output = subprocess.check_output(wait_command, shell=True)
except subprocess.CalledProcessError as e:
  wait_output = e.output
if args.verbose: print "DEBUG: job report file created"
#with open(json_filename, 'r') as f:
if True:
  #matches = re.finditer(regex, f.read(), re.MULTILINE | re.DOTALL)
  matches = re.finditer(regex, wait_output, re.MULTILINE | re.DOTALL)
  for match in matches:
    #print "next block:"
    #print match.group(0)
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
      
if args.verbose: print "DEBUG: Directory so far"
for num in subjobs:
  if args.verbose: print "subjob", num
  subjob = subjobs[num]
  for item in subjob:
    if args.verbose: print "  ", item, "=", subjob[item]
if args.verbose: print ""

# process resubmits
resubmits = 0
for resubmit_cluster,procs in job.resubmits:
  if args.verbose: print "DEBUG: found resubmit clusterid:", resubmit_cluster
  regex = r"\{[^{}]*?(\{.*?\})?[^{}]*?\}"
  #os.system('condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+resubmit_cluster+'.txt > '+json_filename)
  wait_command = 'condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+resubmit_cluster+'.txt'
  output = subprocess.check_output(wait_command, shell=True)
  if args.verbose: print "DEBUG: job report file created"
  resubmits += 1
  #with open(json_filename, 'r') as f:
  if True:
    matches = re.finditer(regex, output, re.MULTILINE | re.DOTALL)
    for match in matches:
      #print "next block:"
      #print match.group(0)
      block = json.loads(match.group(0))
      date = time.strptime(str(block['EventTime']), '%Y-%m-%dT%H:%M:%S')
      t = timegm(date)
      # skip noop_jobs
      if not int(block['Proc']) in procs: continue
      if block['MyType'] == 'SubmitEvent':
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
  if args.verbose: print "DEBUG: Directory so far"
  for num in subjobs:
    if args.verbose: print "subjob", num
    subjob = subjobs[num]
    for item in subjob:
      if args.verbose: print "  ", item, "=", subjob[item]
  if args.verbose: print ""

# Print Status
print "Results for ClusterId", cluster, "at schedd", schedd_name
for count, resubmit_cluster in enumerate(job.resubmits):
  print "  Resubmit", count+1, "ClusterId", resubmit_cluster[0]
print "Job output area:", output_area
if output_eos:
  output = subprocess.check_output('/uscms/home/bchiari1/bin/eosdu -h '+output_area, shell=True)
  size = output.strip()[:-1]
  suffix = output.strip()[-1]
else: # local
  output = subprocess.check_output('du -hs '+output_area, shell=True)
  size = output.split()[0].strip()[:-1]
  suffix = output.split()[0].strip()[-1]
print "Total output size ", size, suffix

if not args.summary: print ' {:<5}| {:<15}| {:<7}| {:<18}| {:<12}| {}'.format(
       'Proc', 'Status', 'Resubs', 'Wall Time', 'Output File', 'Msg'
)
if args.summary: summary = {}
lines = []
for jobNum in subjobs:
  subjob = subjobs[jobNum]
  status = subjob.get('status','')
  reason = subjob.get('reason','')
  reason = reason[0:80]
  if 'start_time' in subjob and 'end_time' in subjob:
    totalTime = str(datetime.timedelta(seconds = timegm(subjob['end_time']) - timegm(subjob['start_time'])))
  elif 'start_time' in subjob:
    totalTime = str(datetime.timedelta(seconds = timegm(time.localtime()) - timegm(subjob['start_time'])))
  elif 'end_time' in subjob:
    totalTime = time.strftime('%m/%d %H:%M:%S', subjob['end_time']) + " (end)"
  else:
    totalTime = ''
  size = subjob.get('size', "")
  if status=='finished' and size=='': status = 'fin w/o output'
  if args.onlyFinished and (status=='submitted' or status=='running'): continue
  if args.onlyError and (status=='submitted' or status=='running' or status=='finished'): continue
  if args.notFinished and (status=='finished'): continue
  resubs = subjob.get('resubmitted', '')
  if resubs == 0: resubs = ''
  if args.onlyResubmits and resubs == '': continue  
  lines.append(' {:<5}| {:<15}| {:<7}| {:<18}| {:<12}| {}'.format(
         str(jobNum), str(status), str(resubs), str(totalTime), str(size), str(reason)
  ))
  if args.summary:
    if status in summary: summary[status] += 1
    else: summary[status] = 1

if not args.summary:
  if args.group:
    def status(line):
      return (line.split('|'))[1]
    lines = sorted(lines, key=status, reverse=True)
  for line in lines:
    print line

if args.summary:
  total = 0
  for key in summary:
    total += summary[key]
  print '{:<15} | {}'.format('Status', 'Job Ids ({} total)'.format(total))
  for status in summary:
    print '{:<15} | {}'.format(str(status), str(summary[status]))
