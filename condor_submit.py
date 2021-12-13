import argparse
import classad
import htcondor
import sys
import subprocess
import os
import copy
import math
from itertools import izip_longest
# command line options
parser = argparse.ArgumentParser(description="")
parser.add_argument("site", 
help="indicate where script is running, hexcms or cmslpc")
parser.add_argument("input", 
help="location containing MiniAODv1 files to process")
input_options = parser.add_mutually_exclusive_group()
input_options.add_argument("--input_hexcms", action="store_true",
help="indicate that input is on hexcms")
input_options.add_argument("--input_cmslpc", action="store_true",
help="indicate that input is on cmslpc")
parser.add_argument("output", 
help="output eos storage to write output to with xrdcp")
output_options = parser.add_mutually_exclusive_group()
output_options.add_argument("--output_hexcms", action="store_true",
help="indicate that output should be written to hexcms")
output_options.add_argument("--output_cmslpc", action="store_true",
help="indicate that output should be written to cmslpc")
parser.add_argument("-d", "--dir", default="my_condor_job",
help="name of job directory, created in current directory")
parser.add_argument("-n", "--num", default=1, type=int,
help="number of subjobs in the job")
parser.add_argument("-v", "--verbose", action="store_true",
help="ask for verbose output")
parser.add_argument("-f", "--force", action="store_true",
help="overwrite job directory if it already exists")
args = parser.parse_args()

# constants
owner = 'chiarito'
submit_file_filename = 'submit_file.jdl'
input_file_filename_base = 'infiles'
finalfile_filename = 'NANOAOD_TwoProng.root'
unpacker_filename = 'unpacker.py'
stageout_filename = 'stageout.py'
stageout_hexcms_template_filename = 'template_stageout_hexcms.py'
unpacker_hexcms_template_filename = 'template_unpacker_hexcms.py'

# subroutines
def grouper(iterable, n, fillvalue=None):
  args = [iter(iterable)] * n
  return izip_longest(*args, fillvalue=fillvalue)

def use_template_to_replace(template_filename, replaced_filename, to_replace):
  with open(template_filename, 'r') as template:
    base = template.read()
  replaced = copy.deepcopy(base)
  replaced += "\n"
  for key in to_replace:
    replaced = replaced.replace(key, to_replace[key])
  with open(replaced_filename, 'w') as temp:
    temp.write(replaced)

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

# make job directory
if os.path.isdir("./"+args.dir) and not args.force:
  raise Exception("Directory " + args.dir + " already exists. Use option -f to overwrite")
if os.path.isdir("./"+args.dir) and args.force:
  subprocess.call(['rm', '-rf', "./"+args.dir])
subprocess.call(['mkdir', args.dir])

# setup based on triple: running location, input location, output location
if args.site == "hexcms" and args.input_hexcms and args.output_hexcms:
  print "\nRunning on hexcms, input data located on hexcms, output written to hexcms\n"
  # discover data
  print "Doing Input Data Discovery"
  cmd = 'ls -1 {}/*'.format(args.input)
  output = subprocess.check_output(cmd, shell=True)
  output = output.split('\n')
  input_files = []
  for line in output:
    if not line.find(".root") == -1:
      input_files.append(line)
      print "  discoverd: ", line
  print ""
  # set scripts
  unpacker_template_filename = unpacker_hexcms_template_filename
  stageout_template_filename = stageout_hexcms_template_filename
      
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

# splitting
input_filenames = []
num_total_files = len(input_files)
num_total_jobs = args.num
num_files_per_job = math.ceil(num_total_files / float(num_total_jobs))
N = int(num_files_per_job)
for count,set_of_lines in enumerate(grouper(input_files, N, '')):
  with open(input_file_filename_base+'_'+str(count)+'.dat', 'w') as fi:
    for line in set_of_lines:
      if line == '': continue
      fi.write(line+'\n')
    input_filenames.append(os.path.basename(fi.name))
for filename in input_filenames:
  os.system('mv ' + filename + ' ' + args.dir)
TOTAL_JOBS = len(input_filenames)

# prepare unpacker script
to_replace = {}
template_filename = unpacker_template_filename
replaced_filename = unpacker_filename
to_replace['__inputfilefilenamebase__'] = input_file_filename_base
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + args.dir)

# prepare stageout script
to_replace = {}
to_replace['__finalfile__'] = finalfile_filename
to_replace['__outputlocation__'] = args.output
template_filename = stageout_template_filename
replaced_filename = stageout_filename
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + args.dir)

# define submit files
sub = htcondor.Submit()
sub['executable'] = 'condor_execute_hexcms.sh'
sub['arguments'] = unpacker_filename + " " + stageout_filename + " $(Process)"
sub['should_transfer_files'] = 'YES'
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
sub['x509userproxy'] = ''
sub['transfer_input_files'] = \
  args.dir+'/'+unpacker_filename + ", " + \
  args.dir+'/'+stageout_filename + ", " + \
  args.dir+'/'+input_file_filename_base+'_$(Process).dat'
sub['transfer_output_files'] = '""'
sub['initialdir'] = ''
sub['output'] = args.dir+'/$(Cluster)_$(Process)_out.txt'
sub['error'] = args.dir+'/$(Cluster)_$(Process)_out.txt'
sub['log'] = args.dir+'/$(Cluster)_log.txt'
with open(submit_file_filename, 'w') as f:
  f.write(sub.__str__())
os.system('mv ' + submit_file_filename + ' ' + args.dir)

# get the schedd
coll = htcondor.Collector()
sched_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
print "Found These Schedds:"
for s in sched_query:
  print "  ", s["Name"]
schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
print "Picked:", schedd_ad["Name"] + "\n"
schedd = htcondor.Schedd(schedd_ad)

# submit the job
print "Submitting Jobs"
with schedd.transaction() as txn:
  print "ClusterId: ", sub.queue(txn, count=TOTAL_JOBS)

# query job
print "\nCurrent Jobs for User:"
for job in schedd.xquery(projection=['ClusterId', 'ProcId', 'JobStatus', 'Owner']):
  if not job['Owner'] == owner: continue
  print job.__repr__()
