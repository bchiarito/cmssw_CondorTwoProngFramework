#!/usr/bin/env python
import argparse
import sys
import subprocess
import os
import copy
import math
import re
import time
import socket
sys.path.append(os.path.join(sys.path[0],'include'))
import dataset_management as dm
from datetime import datetime
from datetime import timedelta
from datetime import date
from itertools import izip_longest

# get site
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')

# constants
helper_dir = 'helper'
executable = 'condor_execute.sh'
src_setup_script = 'cmssw_src_setup.sh'
submit_file_filename = 'submit_file.jdl'
input_file_filename_base = 'infiles' # also assumed in executable
finalfile_filename = 'NANOAOD_TwoProng.root'
unpacker_filename = 'unpacker.py'
stageout_filename = 'stageout.py'
jobinfo_filename = 'job_info.py'
dataset_cache = 'datasets'
fix_condor_hexcms_script = 'hexcms_fix_python.sh'
cmssw_prebuild_area = 'prebuild'

# import condor modules
try:
  import classad
  import htcondor
except ImportError as err:
  if site == 'hexcms':
    raise SystemExit('ERROR: On hexcms, please source this file before running: ' + fix_condor_hexcms_script)
  if site == 'cmslpc':
    print 'ERROR: Could not import classad or htcondor. Verify that python is default and not from cmssw release (do not cmsenv).'
    raise err

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

# command line options
parser = argparse.ArgumentParser(description="")

# input/output
parser.add_argument("input", 
help="Absolute path to local directory/file, cmslpc eos storage (/store/user/...), \
text file (end in .txt) with one file location per line, or dataset name (/*/*/MINIAOD(SIM)).")
input_options = parser.add_mutually_exclusive_group()
input_options.add_argument("--input_local", action="store_true",
help="input is on the local filesystem")
input_options.add_argument("--input_cmslpc", action="store_true",
help="input is an eos area on cmslpc")
input_options.add_argument("--input_dataset", action="store_true",
help="input is an official dataset")
parser.add_argument("output", 
help="Absoulte path to local directory, or cmslpc eos storage (/store/user/...).")
output_options = parser.add_mutually_exclusive_group()
output_options.add_argument("--output_local", action="store_true",
help="output is written to local filesystem")
output_options.add_argument("--output_cmslpc", action="store_true",
help="output is written to an eos area on cmslpc")

# run specification
datamc_options = parser.add_mutually_exclusive_group()
datamc_options.add_argument("--mc", action="store_true",
help="running on mc (default)")
datamc_options.add_argument("--data", action="store_true",
help="running on data")
parser.add_argument("-y", "--year", default="UL18",
help="prescription to follow: UL18 (default), UL17, UL16")
parser.add_argument("--lumiMask", default=None, metavar='', dest='lumiMask',
help="path to lumi mask json file")

# meta-run specification
parser.add_argument("-d", "--dir", default='condor_'+date.today().strftime("%b-%d-%Y"),
help="name of job directory, created in current directory")
parser.add_argument("--batch", metavar='JobBatchName',
help="displays when using condor_q -batch")
parser.add_argument("-n", "--num", default=1, type=int, metavar='INT',
help="number of subjobs in the job (default is 1)")
parser.add_argument("--files", default=-1, type=int, metavar='maxFiles',
help="maximum number of files to include from input area (default is -1, meaning all files)")
parser.add_argument("--useLFN", default=False, action="store_true",
help="when running on dataset do not use xrdcp, instead supply LFN directly to cmssw config")
parser.add_argument("--proxy", default='',
help="location of proxy file, only used on hexcms")

# convenience
parser.add_argument("-f", "--force", action="store_true",
help="overwrite job directory if it already exists")
parser.add_argument("-b", "--rebuild", default=False, action="store_true",
help="remake cmssw prebuild area needed to ship with job")
parser.add_argument("-t", "--test", default=False, action="store_true",
help="don't submit condor jobs but do all other steps")

# end command line options
args = parser.parse_args()

# check data/mc
if not args.mc and not args.data: args.mc = True
if args.mc: datamc = "mc"
elif args.data: datamc = "data"
if args.mc and not args.lumiMask == None:
  raise SystemExit("Configuration Error: Using lumi mask with MC!")
if args.data and args.lumiMask == None:
  raise SystemExit("Configuration Error: Running on data, but provided no lumi mask!")

# check year
if not (args.year == 'UL18' or
        args.year == 'UL17' or
        args.year == 'UL16'):
  raise SystemExit('ERROR: --year must be one of: UL18, UL17, UL16')

# check input
input_not_set = False
if re.match("(?:" + "/.*/.*/MINIAOD" + r")\Z", args.input) or \
   re.match("(?:" + "/.*/.*/MINIAODSIM" + r")\Z", args.input): args.input_dataset = True
if args.input_local == False and args.input_cmslpc == False and args.input_dataset == False:
  input_not_set = True
if input_not_set and site == "hexcms": args.input_local = True
if input_not_set and site == "cmslpc": args.input_cmslpc = True
#print "Checking Input ..."
input_files = [] # each entry a file location
s = args.input
# input is .txt file
if s[len(s)-4:len(s)] == ".txt":
  with open(args.input) as f:
    for line in f:
      input_files.append(line.strip())
      #print "  input file: ", line.strip()
      if len(input_files) == args.files: break
# input is local
elif args.input_local:
  if os.path.isfile(args.input):
    #print "  found local file: ", args.input
    input_files.append(args.input)
    #print ""
  if os.path.isdir(args.input):
    if args.input[len(args.input)-1] == '/': args.input = args.input[0:len(args.input)-1]
    cmd = 'ls -1 {}/*'.format(args.input)
    output = subprocess.check_output(cmd, shell=True)
    output = output.split('\n')
    for line in output:
      if not line.find(".root") == -1:
        input_files.append(line)
        #print "  found local file: ", line
        if len(input_files) == args.files: break
    print ""
# input is eos area on cmslc
elif args.input_cmslpc:
  if s[len(s)-5:len(s)] == ".root":
    #print "  found eos file: ", args.input
    input_files.append(args.input)
  else:
    list_of_files = subprocess.check_output("xrdfs root://cmseos.fnal.gov ls " + args.input, shell=True)
    list_of_files = list_of_files.split('\n')
    for line in list_of_files:
      input_files.append(line)
      #print "  found eos file: ", line
      if len(input_files) == args.files: break
# input is dataset name
elif args.input_dataset:
  dataset_name = args.input
  if not dm.isCached(dataset_name, dataset_cache): dm.process(dataset_name, dataset_cache)
  input_files = dm.getFiles(dataset_name, dataset_cache, args.files)
  #print "  example dataset file: ", input_files[0].strip()
else:
  raise SystemExit('ERROR: Checking input failed! Could not determine input type.')
#print "Processed", len(input_files), "files"
#print "  example file: ", input_files[0].strip()
example_inputfile = str(input_files[0].strip())
ex_in = example_inputfile

# test input
if args.input_cmslpc:
  ret = os.system('eos root://cmseos.fnal.gov ls ' + input_files[0] + ' > /dev/null')
  if not ret == 0:
    raise SystemExit('ERROR: Input is not a valid file on cmslpc eos area! Did you mean to use --input_dataset?')
elif args.input_local:
  if not os.path.isfile(args.input) and not os.path.isdir(args.input):
    raise SystemExit('ERROR: Input is not a valid directory or file on hexcms!')
elif args.input_dataset:
  if False: pass
else:
  raise SystemExit('ERROR: Testing input failed! Could not determine input type.') 

# check output
if args.output[0] == '.':
  raise SystemExit('ERROR: Must use absolute path for output location!')
output_not_set = False
if not args.output[0:7] == '/store/':
  args.output_local = True
elif args.output_local == False and args.output_cmslpc == False:
  output_not_set = True
if output_not_set and site == "hexcms": args.output_local = True
if output_not_set and site == "cmslpc": args.output_cmslpc = True

# create output area
base = args.output
if base[-1] == '/': base = base[:-1]
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
output_path = base+'/'+timestamp
if args.output_local:
  if site == "cmslpc": raise SystemExit('ERROR: Cannot write output to local filesystem when running on cmslpc: functionality not implemented!')
  if not os.path.isdir(output_path):
    ret = os.system('mkdir -p ' + output_path)
    if not ret == 0: raise SystemExit('ERROR: Failed to create job output directory!')
if args.output_cmslpc:
  ret = os.system("eos root://cmseos.fnal.gov mkdir -p " + output_path)
  if not ret == 0: raise SystemExit('ERROR: Failed to create job output directory in cmslpc eos area!')

# test output
if args.output_cmslpc:
  os.system('touch blank.txt')
  ret = os.system('xrdcp --nopbar blank.txt root://cmseos.fnal.gov/' + output_path)
  if not ret == 0: raise SystemExit('ERROR: Failed to xrdcp test file into output eos area!')
  ret = os.system("eos root://cmseos.fnal.gov rm " + output_path + "/blank.txt &> /dev/null")
  if not ret == 0: raise SystemExit('ERROR: Failed eosrm test file from output eos area!')
  os.system('rm blank.txt')

# make job directory
if args.test:
  job_dir = 'TestJob_' + args.dir
  args.force = True
else:
  job_dir = 'Job_' + args.dir
if os.path.isdir("./"+job_dir) and not args.force:
  raise SystemExit("ERROR: Directory " + job_dir + " already exists. Use option -f to overwrite")
if os.path.isdir("./"+job_dir) and args.force:
  os.system('rm -rf ./' + job_dir)
os.system('mkdir ' + job_dir)
os.system('mkdir ' + job_dir + '/infiles')
os.system('mkdir ' + job_dir + '/stdout')

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
      fi.write(line.strip()+'\n')
    input_filenames.append(os.path.basename(fi.name))
  # cmssw_ version of file keeps only filename instead of full path and adds 'file:'
  with open('cmssw_'+input_file_filename_base+'_'+str(count)+'.dat', 'w') as fi:
    for line in set_of_lines:
      if line == '': continue
      if not args.useLFN:
        i = line.rfind('/')
        line = line[i+1:len(line)]
        fi.write('file:'+line.strip()+'\n')
      if args.useLFN:
        fi.write(line.strip()+'\n')
for filename in input_filenames:
  os.system('mv ' + filename + ' ' + job_dir + '/infiles/')
  os.system('mv cmssw_' + filename + ' ' + job_dir + '/infiles/')
TOTAL_JOBS = len(input_filenames)

# prepare unpacker script
template_filename = helper_dir+"/template_"+unpacker_filename
replaced_filename = unpacker_filename
to_replace = {}
to_replace['__inputfilefilenamebase__'] = input_file_filename_base
if args.input_local and site == 'hexcms':
  to_replace['__redirector__'] = ''
  to_replace['__copycommand__'] = 'cp'
if args.input_cmslpc:
  to_replace['__redirector__'] = 'root://cmseos.fnal.gov/'
  to_replace['__copycommand__'] = 'xrdcp --nopbar'
if args.input_dataset:
  to_replace['__redirector__'] = 'root://cmsxrootd.fnal.gov/'
  if args.useLFN: to_replace['__copycommand__'] = 'NULL'
  else: to_replace['__copycommand__'] = 'xrdcp --debug 1 --retry 3 --nopbar'
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + job_dir)

# prepare stageout script
template_filename = helper_dir+"/template_"+stageout_filename
replaced_filename = stageout_filename
to_replace = {}
to_replace['__finalfile__'] = finalfile_filename
to_replace['__outputlocation__'] = output_path
if args.output_local:
  to_replace['__redirector__'] = ''
  to_replace['__copycommand__'] = 'cp'
if args.output_cmslpc:
  to_replace['__redirector__'] = 'root://cmseos.fnal.gov/'
  to_replace['__copycommand__'] = 'xrdcp --nopbar'
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + job_dir)

# prepare prebuild area to send with job
if args.rebuild:
  print "Setting up src directory (inside ./"+cmssw_prebuild_area+") to ship with job"
  os.system('./' + helper_dir +'/'+ src_setup_script)
  print "\nFinished setting up directory to ship with job.\n"
if not args.rebuild and not os.path.isdir(cmssw_prebuild_area):
  raise SystemExit("ERROR: Prebuild area not prepared, use option --rebuild to create")

# define submit files
sub = htcondor.Submit()
sub['executable'] = helper_dir+'/'+executable
sub['arguments'] = unpacker_filename + " " + stageout_filename + " $(Process) " + datamc + " " + args.year
if args.lumiMask is None:
  sub['arguments'] += " None"
else:
  sub['arguments'] += " "+os.path.basename(args.lumiMask)
sub['should_transfer_files'] = 'YES'
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
if site == 'cmslpc': sub['use_x509userproxy'] = 'true'
if site == 'hexcms': sub['x509userproxy'] = os.path.basename(args.proxy)
sub['transfer_input_files'] = \
  job_dir+'/'+unpacker_filename + ", " + \
  job_dir+'/'+stageout_filename + ", " + \
  job_dir+'/infiles/'+input_file_filename_base+'_$(Process).dat' + ", " + \
  job_dir+'/infiles/'+'cmssw_'+input_file_filename_base+'_$(Process).dat' + ", " + \
  cmssw_prebuild_area+'/CMSSW_10_6_20/src/PhysicsTools' + ", " + \
  cmssw_prebuild_area+'/CMSSW_10_6_20/src/CommonTools'
if not args.lumiMask is None:
  sub['transfer_input_files'] += ", "+args.lumiMask
sub['transfer_output_files'] = '""'
sub['on_exit_remove'] = '((ExitBySignal == False) && (ExitCode == 0)) || (NumJobStarts >= 3)'
sub['initialdir'] = ''
sub['JobBatchName'] = args.dir if args.batch is None else args.batch
sub['output'] = job_dir+'/stdout/$(Cluster)_$(Process)_out.txt'
sub['error'] = job_dir+'/stdout/$(Cluster)_$(Process)_out.txt'
sub['log'] = job_dir+'/log_$(Cluster).txt'

# copy files to job diretory 
command = ''
for a in sys.argv:
  command += a + ' '
with open(submit_file_filename, 'w') as f:
  f.write(sub.__str__())
  f.write('\n# Command:\n#'+command)
os.system('mv ' + submit_file_filename + ' ' + job_dir)
os.system('cp ' + helper_dir +'/'+ executable + ' ' + job_dir)

# check proxy
if site == 'hexcms' and args.input_dataset and args.proxy == '':
  raise SystemExit("ERROR: No grid proxy provided! Please use command voms-proxy-info and provide 'path' variable to --proxy")
if site == 'hexcms' and args.input_dataset and not args.proxy == '':
  if not os.path.isfile(os.path.basename(args.proxy)):
    os.system('cp '+args.proxy+' .')
if site == 'cmslpc':
  time_left = str(timedelta(seconds=int(subprocess.check_output("voms-proxy-info -timeleft", shell=True))))

# get the schedd
coll = htcondor.Collector()
sched_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
if site == 'hexcms': schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
if site == 'cmslpc': schedd_ad = sched_query[0]
schedd = htcondor.Schedd(schedd_ad)

# print summary
if args.output_local: o_assume = 'local'
if args.output_cmslpc: o_assume = 'cmslpc eos'
if args.input_local: i_assume = 'local'
if args.input_cmslpc: i_assume = 'cmslpc eos'
if args.input_dataset: i_assume = 'official dataset'
print "Summary"
print "-------"
print "Job Directory       :", job_dir
print "Job Specification   :", args.year +" "+datamc.upper()
print "Total Jobs          :", str(TOTAL_JOBS)
print "Total Files         :", str(num_total_files)
print "Files/Job (approx)  :", str(N)
print "Input               : " + i_assume
if len(input_files)>1: print "Example Input File  : " + ((ex_in[:88] + '..') if len(ex_in) > 90 else ex_in)
else : print "Input File          : " + ((ex_in[:88] + '..') if len(ex_in) > 90 else ex_in)
print "Output              : " + o_assume
print "Output Directory    :", output_path
if not args.lumiMask is None:
  print "Lumi Mask           : " + os.path.basename(args.lumiMask)
print "Schedd              :", schedd_ad["Name"]
if site=='cmslpc': print "Grid Proxy          :", time_left + ' left'

# premature exit for test
if args.test:
  print "Just a test, Exiting."
  sys.exit()

# prompt user to double-check job summary
while True:
  response = raw_input("Please check summary. [Enter] to proceed with submission, q to quit: ")
  if response == 'q':
    print "Quitting."
    os.system('rm -rf '+job_dir)
    sys.exit()
  elif response == '': break
  else: pass

# submit the job
print "Submitting Jobs ..."
with schedd.transaction() as txn:
  cluster_id = sub.queue(txn, count=TOTAL_JOBS)
  print "ClusterId: ", cluster_id

# prepare job_info.py file
template_filename = helper_dir+"/template_"+jobinfo_filename
replaced_filename = jobinfo_filename
to_replace = {}
to_replace['__cluster__'] = str(cluster_id)
to_replace['__queue__'] = str(TOTAL_JOBS)
to_replace['__schedd__'] = schedd_ad["Name"]
to_replace['__output__'] = output_path
use_template_to_replace(template_filename, replaced_filename, to_replace)
os.system('mv ' + replaced_filename + ' ' + job_dir)
