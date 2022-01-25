import argparse
import classad
import htcondor
import sys
import subprocess
import os
import copy
import math
import re
import time
import socket
from itertools import izip_longest

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

# constants
executable = 'condor_execute.sh'
src_setup_script = 'cmssw_src_setup.sh'
submit_file_filename = 'submit_file.jdl'
input_file_filename_base = 'infiles' # also assumed in executable
finalfile_filename = 'NANOAOD_TwoProng.root'
unpacker_filename = 'unpacker.py'
stageout_filename = 'stageout.py'
unpacker_template_filename = 'template_unpacker.py'
stageout_template_filename = 'template_stageout.py'

# command line options
parser = argparse.ArgumentParser(description="")

# input/output
parser.add_argument("input", 
help="directory containing MiniAOD files, or a single MiniAOD file, \
or .txt file with file locations (/store/user/... or /abs/path/to/file.root) one per line, \
or dataset name (/*/*/MINIAOD(SIM)). the --input_* options can be used to overwrite \
automatic assumptions about input location.")
input_options = parser.add_mutually_exclusive_group()
input_options.add_argument("--input_local", action="store_true",
help="indicate that input is on the local filesystem")
input_options.add_argument("--input_cmslpc", action="store_true",
help="indicate that input is an eos area on cmslpc")
input_options.add_argument("--input_dataset", action="store_true",
help="indicate that input is official dataset")
parser.add_argument("output", 
help="local directory or eos storage (/store/user/...) to write output to, \
the --output_* options can be used to overwite automatic assumptons \
about output location.")
output_options = parser.add_mutually_exclusive_group()
output_options.add_argument("--output_local", action="store_true",
help="indicate that output should be written to local filesystem")
output_options.add_argument("--output_cmslpc", action="store_true",
help="indicate that output should be written to an eos area on cmslpc")

# job structure
parser.add_argument("-n", "--num", default=1, type=int,
help="number of subjobs in the job (default is 1)")
parser.add_argument("-d", "--dir", default="my_condor_job",
help="name of job directory, created in current directory")
parser.add_argument("-f", "--force", action="store_true",
help="overwrite job directory if it already exists")
parser.add_argument("-b", "--rebuild", default=False, action="store_true",
help="remake scratch directory and src/ area needed to ship with job")
parser.add_argument("-t", "--test", default=False, action="store_true",
help="don't submit condor job but do all other steps")
parser.add_argument("--useLFN", default=False, action="store_true",
help="use only with official dataset, do not xrdcp and instead supply LFN directly to cmssw config")

# convenience
parser.add_argument("-s", "--short", default=False, action="store_true",
help="for a short job, wait for it using condor_wait")
parser.add_argument("--nocleanup", default=False, action="store_true",
help="do not cleanup job directory after job starts running")

# not used yet
parser.add_argument("-y", "--year", default="UL18",
help="prescription to follow, e.g., UL18, UL17, UL16")
mc_options = parser.add_mutually_exclusive_group()
mc_options.add_argument("--mc", action="store_true",
help="running on mc")
mc_options.add_argument("--data", action="store_true",
help="running on data")
parser.add_argument("-v", "--verbose", action="store_true",
help="ask for verbose output")

# end command line options
args = parser.parse_args()

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise Exception('Unrecognized site: not hexcms, cmslpc, or lxplus')

# check input
input_not_set = False
if args.input_local == False and args.input_cmslpc == False and args.input_dataset == False:
  input_not_set = True
if input_not_set and site == "hexcms": args.input_local = True
if input_not_set and site == "cmslpc": args.input_cmslpc = True
print "Checking Input ..."
input_files = [] # each entry a file location
s = args.input
txt_file = False
# input is .txt file
if s[len(s)-4:len(s)] == ".txt":
  print "Found input .txt file with file locations"
  with open(args.input) as f:
    for line in f:
      input_files.append(line.strip())
      print "  input file: ", line.strip()
  txt_file = True
# input is directory on hexcms, and running on hexcms
if not txt_file and args.input_local and site == "hexcms":
  if os.path.isfile(args.input):
    print "  found local file: ", args.input
    input_files.append(args.input)
    print ""
  if os.path.isdir(args.input):
    if args.input[len(args.input)-1] == '/': args.input = args.input[0:len(args.input)-1]
    cmd = 'ls -1 {}/*'.format(args.input)
    output = subprocess.check_output(cmd, shell=True)
    output = output.split('\n')
    for line in output:
      if not line.find(".root") == -1:
        input_files.append(line)
        print "  found local file: ", line
    print ""
# input is directory on hexcms, and running on cmslpc
if not txt_file and args.input_local and site == "cmslpc":
  raise Exception("Not supported running on cmslpc with input data on hexcms.")
# input is eos area on cmslc
if not txt_file and args.input_cmslpc:
  if s[len(s)-5:len(s)] == ".root":
    print "  found eos file: ", args.input
    input_files.append(args.input)
  else:
    list_of_files = subprocess.check_output("xrdfs root://cmseos.fnal.gov ls -u " + args.input, shell=True)
    list_of_files = list_of_files.split('\n')
    for line in list_of_files:
      input_files.append(line)
      print "  found eos file: ", line
# input is dataset name
if re.match("(?:" + "/.*/.*/MINIAOD" + r")\Z", args.input) or \
   re.match("(?:" + "/.*/.*/MINIAODSIM" + r")\Z", args.input):
  args.input_dataset = True
  # check if txt file exists, if not generate it
  # use txt file to fill input_files, just line by line
  raise Exception('Intput dataset name directorly not implemented!')

# test input
if args.input_cmslpc:
  ret = os.system('eos root://cmseos.fnal.gov ls ' + input_files[0] + ' > /dev/null')
  if not ret == 0:
    raise Exception('Input is not a valid file on cmslpc eos area!')
if args.input_local:
  if not os.path.isfile(args.input) and not os.path.isdir(args.input):
    raise Exception('Input is not a valid directory or file on hexcms!')
if args.input_dataset:
  if False: raise Exception()

# check output
output_not_set = False
if not args.output[0] == '/':
  args.output_local = True
elif args.output_local == False and args.output_cmslpc == False:
  output_not_set = True
if output_not_set and site == "hexcms": args.output_local = True
if output_not_set and site == "cmslpc": args.output_cmslpc = True
if args.output_local:
  if not os.path.isdir(args.output):
    print "\nMaking output directory, because it does not already exist..."
    ret = os.system('mkdir -p ' + args.output)
    if not ret == 0: raise Exception('Failed to create job output directory!')
if site == "cmslpc" and args.output_cmslpc:
  print "\nEnsuring output eos location exists..."
  ret = os.system("eos root://cmseos.fnal.gov mkdir -p " + args.output)
  if not ret == 0: raise Exception('Failed to create job output directory!')
print ""

# print input/output assumptions
if args.output_local: o = 'local'
if args.output_cmslpc: o = 'cmslpc eos'
if args.input_local: i = 'local'
if args.input_cmslpc: i = 'cmslpc eos'
if args.input_dataset: i = 'official dataset'
print "Job Locations"
print "Input : " + i
print "Output: " + o + "\n"

# make job directory
job_dir = 'Job_' + args.dir
if os.path.isdir("./"+job_dir) and not args.force:
  raise Exception("Directory " + job_dir + " already exists. Use option -f to overwrite")
if os.path.isdir("./"+job_dir) and args.force:
  #subprocess.call(['rm', '-rf', "./"+job_dir])
  os.system('rm -rf ./' + job_dir)
print "Job Directory :", job_dir
#subprocess.call(['mkdir', job_dir])
os.system('mkdir ' + job_dir)
print ""

# splitting
num_total_files = len(input_files)
num_total_jobs = args.num
num_files_per_job = math.ceil(num_total_files / float(num_total_jobs))
N = int(num_files_per_job)

# prepare input file files
input_filenames = [] # each entry a filename, and the file is a txt file of input filenames one per line
for count,set_of_lines in enumerate(grouper(input_files, N, '')):
  with open(input_file_filename_base+'_'+str(count)+'.dat', 'w') as fi:
    for line in set_of_lines:
      if line == '': continue
      fi.write(line+'\n')
    input_filenames.append(os.path.basename(fi.name))
  # cmssw_ version of file keeps only filename instead of full path and adds 'file:'
  with open('cmssw_'+input_file_filename_base+'_'+str(count)+'.dat', 'w') as fi:
    for line in set_of_lines:
      if line == '': continue
      if not args.useLFN:
        i = line.rfind('/')
        line = line[i+1:len(line)]
        fi.write('file:'+line+'\n')
      if args.useLFN:
        fi.write(line+'\n')
for filename in input_filenames:
  os.system('mv ' + filename + ' ' + job_dir)
  os.system('mv cmssw_' + filename + ' ' + job_dir)
TOTAL_JOBS = len(input_filenames)

# prepare unpacker script
to_replace = {}
template_filename = unpacker_template_filename
replaced_filename = unpacker_filename
to_replace['__inputfilefilenamebase__'] = input_file_filename_base
if args.input_local and site == 'hexcms':
  to_replace['__redirector__'] = ''
  to_replace['__copycommand__'] = 'cp'
if args.input_cmslpc:
  to_replace['__redirector__'] = 'root://cmseos.fnal.gov/'
  to_replace['__copycommand__'] = 'xrdcp --nopbar'
if args.input_dataset:
  to_replace['__redirector__'] = 'root://cmsxrootd.fnal.gov/'
  if args.useLFN: to_replace['__copycommand__'] = 'echo'
  else: to_replace['__copycommand__'] = 'xrdcp --nopbar'
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + job_dir)

# prepare stageout script
to_replace = {}
to_replace['__finalfile__'] = finalfile_filename
to_replace['__outputlocation__'] = args.output
if args.output_local:
  to_replace['__redirector__'] = ''
  to_replace['__copycommand__'] = 'cp'
if args.output_cmslpc:
  to_replace['__redirector__'] = 'root://cmseos.fnal.gov/'
  to_replace['__copycommand__'] = 'xrdcp --nopbar'
template_filename = stageout_template_filename
replaced_filename = stageout_filename
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + job_dir)

# prepare src/ setup area to send with job, if not already built
if args.rebuild:
  print "Setting up src directory (inside ./scratch/) to ship with job"
  os.system('./' + src_setup_script)
  print "\nFinished setting up scratch directory to ship with job.\n"
if not args.rebuild and not os.path.isdir('scratch'):
  raise Exception("Scratch area not prepared, use option --rebuild to create")

# define submit files
sub = htcondor.Submit()
sub['executable'] = executable
sub['arguments'] = unpacker_filename + " " + stageout_filename + " $(Process)"
sub['should_transfer_files'] = 'YES'
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
if site == 'cmslpc': sub['use_x509userproxy'] = 'true'
if site == 'hexcms': sub['x509userproxy'] = ''
sub['transfer_input_files'] = \
  job_dir+'/'+unpacker_filename + ", " + \
  job_dir+'/'+stageout_filename + ", " + \
  job_dir+'/'+input_file_filename_base+'_$(Process).dat' + ", " + \
  job_dir+'/'+'cmssw_'+input_file_filename_base+'_$(Process).dat' + ", " + \
  'scratch/CMSSW_10_6_20/src/PhysicsTools' + ", " + \
  'scratch/CMSSW_10_6_20/src/CommonTools'
sub['transfer_output_files'] = '""'
sub['initialdir'] = ''
sub['output'] = job_dir+'/$(Cluster)_$(Process)_out.txt'
sub['error'] = job_dir+'/$(Cluster)_$(Process)_out.txt'
sub['log'] = job_dir+'/$(Cluster)_log.txt'

# move copy of submit file and executable to job diretory 
command = ''
for a in sys.argv:
  command += a + ' '
with open(submit_file_filename, 'w') as f:
  f.write(sub.__str__())
  f.write('\n# Command:\n#'+command)
os.system('mv ' + submit_file_filename + ' ' + job_dir)
os.system('cp ' + executable + ' ' + job_dir)

# get the schedd
coll = htcondor.Collector()
sched_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
print "Found These Schedds:"
for s in sched_query:
  print "  ", s["Name"]
if site == 'hexcms': schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
if site == 'cmslpc': schedd_ad = sched_query[0]
print "Picked:", schedd_ad["Name"] + "\n"
schedd = htcondor.Schedd(schedd_ad)

# submit the job
if args.test:
  print "Just a test, Exiting."
  os.system('rm -rf ' + job_dir)
  sys.exit()
print "Submitting Jobs ..."
with schedd.transaction() as txn:
  cluster_id = sub.queue(txn, count=TOTAL_JOBS)
  print "ClusterId: ", cluster_id

# condor_wait for the job
if args.short:
  print "Waiting on job with condor_wait ...\n"
  time.sleep(1)
  os.system('condor_wait -echo ' + job_dir + '/' + str(cluster_id) + '_log.txt')

# cleanup job directory
if not args.nocleanup:
  time.sleep(60)
  for filename in input_filenames:
    os.system('rm ' + job_dir + '/' + filename)
    os.system('rm ' + job_dir + '/cmssw_' + filename)
