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
parser.add_argument("-v", "--verbose", default=False, action="store_true",help="turn on debug messages")
parser.add_argument("-s", "--short", default=False, action="store_true",help="make Msg shorter")
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
    job = int(fi[len(fi)-6])
    subjobs[job]['size'] = size
  except (IndexError, ValueError):
    print "WARNING: got IndexError or ValueError, may want to check output area directly with (eos) ls."
    continue

# parse json job report
if args.verbose: print "DEBUG: parse job report file, creating with condor_wait ..."
#regex = r"\{(.*?)\}"
regex = r"\{[^{}]*?(\{.*?\})?[^{}]*?\}"
os.system('condor_wait -echo:JSON -wait 0 '+args.jobDir+'/log_'+cluster+'.txt > '+json_filename)
if args.verbose: print "DEBUG: job report file created"
with open(json_filename, 'r') as f:
  matches = re.finditer(regex, f.read(), re.MULTILINE | re.DOTALL)
  for match in matches:
    #print "next block:"
    #print match.group(0)
    block = json.loads(match.group(0))
    #print "summary:"
    #print block['MyType']
    #print block['EventTime']
    #print block['Proc']
    date = time.strptime(str(block['EventTime']), '%Y-%m-%dT%H:%M:%S')
    t = timegm(date)
    #print t
    #print "done."
    if block['MyType'] == 'SubmitEvent':
      subjobs[int(block['Proc'])]['start_time'] = date
      subjobs[int(block['Proc'])]['status'] = 'submitted'
    if block['MyType'] == 'ExecuteEvent':
      subjobs[int(block['Proc'])]['start_time'] = date
      subjobs[int(block['Proc'])]['status'] = 'running'
    if block['MyType'] == 'JobTerminatedEvent':
      subjobs[int(block['Proc'])]['end_time'] = date
      if block['TerminatedNormally']:
        subjobs[int(block['Proc'])]['status'] = 'finished'
      else:
        subjobs[int(block['Proc'])]['status'] = 'failed'
    if block['MyType'] == 'JobAbortedEvent':
      subjobs[int(block['Proc'])]['end_time'] = date
      subjobs[int(block['Proc'])]['reason'] = block['Reason']
      subjobs[int(block['Proc'])]['status'] = 'aborted'
    if block['MyType'] == 'JobHeldEvent':
      subjobs[int(block['Proc'])]['end_time'] = date
      subjobs[int(block['Proc'])]['reason'] = block['HoldReason']    
      subjobs[int(block['Proc'])]['status'] = 'held'
    #raw_input()
      
if args.verbose: print "DEBUG: Directory so far"
for num in subjobs:
  if args.verbose: print "subjob", num
  subjob = subjobs[num]
  for item in subjob:
    if args.verbose: print "  ", item, "=", subjob[item]
if args.verbose: print ""

# Print Status
print "Results for ClusterId", cluster, "at schedd", schedd_name
print "Job output area:", output_area
print ' {:<5}| {:<10}| {:<21}| {:<20}| {}'.format(
       'Proc', 'Status', 'Run Time', 'Output File Size', 'Msg'
)
for jobNum in subjobs:
  subjob = subjobs[jobNum]
  status = subjob.get('status','')
  reason = subjob.get('reason','')
  if not args.short: reason = reason[0:80]
  else: reason = reason[0:40]
  if 'start_time' in subjob and 'end_time' in subjob:
    totalTime = str(datetime.timedelta(seconds = timegm(subjob['end_time']) - timegm(subjob['start_time'])))
  elif 'start_time' in subjob:
    totalTime = str(datetime.timedelta(seconds = timegm(time.localtime()) - timegm(subjob['start_time'])))
    #print subjob['start_time']
    #print time.localtime()
    #print calendar.timegm(subjob['start_time'])
    #print calendar.timegm(time.localtime())
    #print calendar.timegm(time.localtime()) - calendar.timegm(subjob['start_time'])
  elif 'end_time' in subjob:
    totalTime = time.strftime('%m/%d %H:%M:%S', subjob['end_time']) + " (end)"
  else:
    totalTime = ''
  size = subjob.get('size', "")
  print ' {:<5}| {:<10}| {:<21}| {:<20}| {}'.format(
         str(jobNum), str(status), str(totalTime), str(size), str(reason)
  )
os.system('rm '+json_filename)

# print current jobs
#ads = schedd.query(
#  constraint='ClusterId =?= {}'.format(cluster),
#  projection=["ClusterId", "ProcId", "JobStatus"],
#)
#print "CURRENT JOBS"
#for ad in ads:
#  print ad['ClusterId'], ad['ProcID'], ad['JobStatus']

# print past jobs
#ads = schedd.history(
#  constraint='ClusterId =?= {}'.format(cluster),
#  projection=["ClusterId", "ProcId", "JobStatus"],
#  match=10
#)
#print "PAST JOBS"
#for ad in ads:
#  print ad['ClusterId'], ad['ProcID'], ad['JobStatus']
