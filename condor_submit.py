import argparse
import classad
import htcondor
import sys
import subprocess
# command line options
parser = argparse.ArgumentParser(description="")
parser.add_argument("site", help="indicate where script is running, hexcms or cmslpc")
parser.add_argument("input", help="location containing MiniAODv1 files to process", )
input_options = parser.add_mutually_exclusive_group()
input_options.add_argument("--input_hexcms", help="indicate that input is on hexcms", action="store_true")
input_options.add_argument("--input_cmslpc", help="indicate that input is on cmslpc", action="store_true")
parser.add_argument("output", help="output eos storage to write output to with xrdcp")
output_options = parser.add_mutually_exclusive_group()
output_options.add_argument("--output_hexcms", help="indicate that output should be written to hexcms", action="store_true")
output_options.add_argument("--output_cmslpc", help="indicate that output should be written to cmslpc", action="store_true")
parser.add_argument("-d", "--dir", help="name of job directory, created in current directory", default="my_condor_job")
parser.add_argument("-n", "--num", help="number of subjobs in the job", default=1)
args = parser.parse_args()
# check valid site
if not args.site == "hexcms" and not args.site == "cmslpc":
  raise Exception("Error: site must be 'hexcms' or 'cmslpc'")
# default choices for location of input and output
if args.input_hexcms == False and args.input_cmslpc == False:
  if args.site == "hexcms": args.input_hexcms = True
  if args.site == "cmslpc": args.input_cmslpc = True
if args.output_hexcms == False and args.output_cmslpc == False:
  if args.site == "hexcms": args.output_hexcms = True
  if args.site == "cmslpc": args.output_cmslpc = True

# get the schedd
coll = htcondor.Collector()
sched_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
print "Found these scheds:"
for s in sched_query:
  print "  ", s["Name"]
schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
print "Picked:", schedd_ad["Name"] + "\n"
schedd = htcondor.Schedd(schedd_ad)

# discovering data
if args.site == "hexcms" and args.input_hexcms and args.output_hexcms:
  print "Running on hexcms, input data located on hexcms, output written to hexcms"
  print args.input
  os.system("ls -1 %s* | grep .root".format(args.input))
  output = subprocess.check_output("cat /etc/services", shell=True)
  
if args.site == "hexcms" and args.input_hexcms and args.output_cmslpc:
  print "Running on hexcms, input data located on hexcms, output written to hexcms"
  raise Exception("Unsupported Combination of input and output locations")
if args.site == "hexcms" and args.input_cmslpc and args.output_hexcms:
  print "Running on hexcms, input data located on cmslpc, output written to hexcms"
  raise Exception("Unsupported Combination of input and output locations")
if args.site == "hexcms" and args.input_cmslpc and args.output_cmslpc:
  print "Running on hexcms, input data located on cmslpc, output written to cmslpc"
  raise Exception("Unsupported Combination of input and output locations")
  
if args.site == "cmslpc" and args.input_hexcms and args.output_hexcms:
  print "Running on cmslpc, input data located on hexcms, output written to hexcms"
if args.site == "cmslpc" and args.input_hexcms and args.output_cmslpc:
  print "Running on cmslpc, input data located on hexcms, output written to cmslpc"
if args.site == "cmslpc" and args.input_cmslpc and args.output_hexcms:
  print "Running on cmslpc, input data located on cmslpc, output written to hexcms"
if args.site == "cmslpc" and args.input_cmslpc and args.output_cmslpc:
  print "Running on cmslpc, input data located on cmslpc, output written to cmslpc"
#cmd = 'xrdfs root://ruhex-osgce.rutgers.edu/ ls ' + args.input_path
#import os
#print cmd
#os.system(cmd)

sys.exit()

# submit job
sub = htcondor.Submit()
sub['executable'] = '/bin/sleep'
sub['arguments'] = '5m'
with schedd.transaction() as txn:
  print sub.queue(txn)            # Queues one job in the current transaction; returns job's cluster ID

# query job
for job in schedd.xquery(projection=['ClusterId', 'ProcId', 'JobStatus']):
  print job.__repr__()


print '''
1: Idle (I)
2: Running (R)
3: Removed (X)
4: Completed (C)
5: Held (H)
6: Transferring Output
7: Suspended
'''

