import os
import sys
import argparse
import subprocess
import classad
import htcondor

parser = argparse.ArgumentParser(description="")
parser.add_argument("jobDir",help="the condor_submit.py job directory")
args = parser.parse_args()

# import job
sys.path.append(args.jobDir)
import job_info as job
cluster = job.cluster
procs = job.procs
schedd_name = job.schedd_name
output_area = job.output

# get the schedd
coll = htcondor.Collector()
schedd_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
for s in schedd_query:
  if str(s["Name"]) == str(schedd_name):
    schedd_ad = s
schedd = htcondor.Schedd(schedd_ad)
print schedd

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

# discover subjob jobNums
subjobs = {}
for proc in procs:
  subjob = {}
  subjobs[proc] = subjob

# discover output files
output = subprocess.check_output('eos root://cmseos.fnal.gov ls -lh '+output_area, shell=True)
print output
for line in output.split('\n'):
  #print len(line.split())
  #for item in line.split():
  # print "  ", item
  if len(line.split()) == 0: continue
  l = line.split()
  fi = l[9]
  #print l[4], l[5], l[9], fi[len(fi)-6]
  size = l[4]+' '+l[5]
  job = int(fi[len(fi)-6])
  subjobs[job]['size'] = size

print "DEBUG: Directory so far"
for num in subjobs:
  print "subjob", num
  subjob = subjobs[num]
  for item in subjob:
    print "  ", item, subjob[item]
print ""

print "Results for ClusterId", cluster, "at schedd", schedd_name
print "Job output area:", output_area
# print status
for num in subjobs:
  subjob = subjobs[num]
  # test
  #current_ad = schedd.query(
  #  constraint='ClusterId =?= {} && ProcId =?= {}'.format(str(cluster), str(subjob['jobNum'])),
  #  projection=["ClusterId", "ProcId", "JobStatus"],
  #)
  #past_ad = schedd.history(
  #  constraint='ClusterId =?= {} && ProcId =?= {}'.format(str(cluster), str(subjob)),
  #  projection=["ClusterId", "ProcId", "JobStatus"],
  #  match=10
  #)
  #if not len(current_ad) == 0: print "  current job", current_ad[0]["JobStatus"]
  #for past in past_ad:
  #  print "past job", past_ad[0]["JobStatus"]
  jobNum = num
  finished = ' '
  totalTime = ' '
  hasOutput = 'Y' if ('size' in subjob) else 'N'
  size = subjob.get('size', "")
  events_in = 0
  events_out = 0
  missing = events_in - events_out
  print ' {:<5}| {:<6}| {:<10}| {:<3}| {:<14}| {}'.format(
         str(jobNum), str(finished), str(totalTime), str(hasOutput), str(size), str(events_in)+' ('+str(missing)+')'
  )
