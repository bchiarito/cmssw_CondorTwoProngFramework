#!/usr/bin/env python3
from __future__ import print_function
import argparse
import sys
import subprocess
import os
import copy
import math
import re
import time
import socket
from datetime import datetime, timedelta, date
from itertools import zip_longest
sys.path.append(os.path.join(sys.path[0],'include'))
import dataset_management as dm
if hasattr(__builtins__, 'raw_input'):
    input = raw_input

# constants
helper_dir = 'helper'
executable = 'condor_execute.sh'
executable_fast = 'condor_execute_fast.sh'
src_setup_script = 'prebuild_setup.sh' # also in unit test scripts
submit_file_filename = 'submit_file.jdl'
input_file_filename_base = 'infiles' # also in executable
finalfile_filename = 'NANOAOD_TwoProng.root'
unpacker_filename = 'unpacker.py'
stageout_filename = 'stageout.py'
jobinfo_filename = 'job_info.py'
dataset_cache = 'datasets'
fix_condor_hexcms_script = 'hexcms_fix_python.sh'
hexcms_proxy_script = 'hexcms_proxy_setup.sh'
hexcms_proxy_script_timeleft = 'hexcms_proxy_timeleft.sh'
cmssw_prebuild_area = 'prebuild'

# subroutines
def grouper(iterable, n, fillvalue=None):
  args = [iter(iterable)] * n
  return zip_longest(*args, fillvalue=fillvalue)
def use_template_to_replace(template_filename, replaced_filename, to_replace):
  with open(template_filename, 'rt') as template:
    base = template.read()
  replaced = copy.deepcopy(base)
  replaced += "\n"
  for key in to_replace:
    replaced = replaced.replace(key, to_replace[key])
  with open(replaced_filename, 'wt') as temp:
    temp.write(replaced)

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

# command line options
parser = argparse.ArgumentParser(description="", usage="./%(prog)s INPUT OUTPUT [--data/mc/sigRes/sigNonRes] -y ULYY -l JSON -d DIR")

# input/output
io_args = parser.add_argument_group('input/output options')
io_args.add_argument("input", metavar='INPUT',
help="Absolute path to local directory/file, cmslpc eos storage (/store/user/...), \
text file (end in .txt) with one file location per line, or dataset name (/*/*/MINIAOD(SIM)).")
input_options = io_args.add_mutually_exclusive_group()
input_options.add_argument("--input_local", action="store_true",
help=argparse.SUPPRESS)
input_options.add_argument("--input_cmslpc", action="store_true",
help=argparse.SUPPRESS)
input_options.add_argument("--input_dataset", action="store_true",
help=argparse.SUPPRESS)
io_args.add_argument("output", metavar='OUTPUT',
help="Absoulte path to local directory, or cmslpc eos storage (/store/user/...).")
output_options = io_args.add_mutually_exclusive_group()
output_options.add_argument("--output_local", action="store_true",
help=argparse.SUPPRESS)
output_options.add_argument("--output_cmslpc", action="store_true",
help=argparse.SUPPRESS)

# execution specification
exec_args = parser.add_argument_group('execution options')
datamc_options = exec_args.add_mutually_exclusive_group()
datamc_options.add_argument("--data", action="store_true", default=False,
help="running on data")
datamc_options.add_argument("--mc", action="store_true", default=False, 
help="running on bkg mc")
datamc_options.add_argument("--sigRes", action="store_true", default=False, 
help="running on resonant signal mc")
datamc_options.add_argument("--sigNonRes", action="store_true", default=False,
help="running on nonresonant signal mc")
exec_args.add_argument("-y", "--year", default="UL18", choices=['UL16','UL17','UL18'], metavar='ULYY',
help="prescription to follow: UL18 (default), UL17, UL16")
exec_args.add_argument("-l", "--lumiMask", default=None, metavar='', dest='lumiMask',
help="path to lumi mask json file")
exec_args.add_argument("--twoprongSB", default="None", choices=['None','full'], metavar='CHOICE',
help="include twoprong sideband: None (default), full")
exec_args.add_argument("--twoprongExtra", action="store_true", default=False,
help="modify twoprong object: allow optional extra track")
exec_args.add_argument("--photonSB", default="None", choices=['None'], metavar='CHOICE',
help="include photon sideband (default None)")
exec_args.add_argument("--selection", default="None", choices=['None', 'muon', 'photon'], metavar='CHOICE',
help="apply event preselection None (default), muon, photon")
exec_args.add_argument("--noPayload", default=False, action="store_true",
help="for testing purposes")

# run specification
run_args = parser.add_argument_group('run options')
run_args.add_argument("-d", "--dir", default='condor_'+date.today().strftime("%b-%d-%Y"),
help="name of job directory, created in current directory")
run_args.add_argument("-b", "--batch", metavar='STR',
help="displays when using condor_q -batch")
num_options = run_args.add_mutually_exclusive_group()
num_options.add_argument("--numJobs", type=int, metavar='INT',
help="total number of subjobs in the job")
num_options.add_argument("--filesPerJob", type=int, metavar='INT', default=None,
help="number of files per subjob (default is 1)")
run_args.add_argument("--files", default=-1, type=float, metavar='maxFiles',
help="total files, <1 treated as a fraction e.g. 0.1 means 10%% (default is all)")
run_args.add_argument("--trancheMax", type=int, metavar='INT', default=50000,
help=argparse.SUPPRESS)
run_args.add_argument("--scheddLimit", type=int, metavar='INT', default=-1,
help="maximum total idle + running on schedd")
run_args.add_argument("--useLFN", default=False, action="store_true",
help="do not use xrdcp, supply LFN directly to cmssw cfg")

# convenience
other_args = parser.add_argument_group('misc options')
other_args.add_argument("-f", "--force", action="store_true",
help="overwrite job directory if it already exists")
other_args.add_argument("--rebuild", default=False, action="store_true",
help="remake cmssw prebuild area needed to ship with job")
other_args.add_argument("-t", "--test", default=False, action="store_true",
help="don't submit condor jobs but do all other steps")
other_args.add_argument("-v", "--verbose", default=False, action="store_true",
help="activate debug output")
other_args.add_argument("--proxy", default='',
help="location of proxy file, only used on hexcms")
other_args.add_argument("--noErr", default=False, action="store_true",
help="do not save stderr in log files")

# end command line options
args = parser.parse_args()

# check data/mc
if args.mc: datamc = "mc"
elif args.data: datamc = "data"
elif args.sigRes: datamc = "sigRes"
elif args.sigNonRes: datamc = "sigNonRes"
else: raise SystemExit("Missing Option: Specification of --data / --mc / --sigRes / --sigNonRes required!")
if datamc != 'data' and not args.lumiMask == None:
  raise SystemExit("Configuration Error: Using lumi mask with MC!")
if datamc == 'data' and args.lumiMask == None:
  raise SystemExit("Configuration Error: Running on data, but provided no lumi mask!")

# check year
if not (args.year == 'UL18' or
        args.year == 'UL17' or
        args.year == 'UL16'):
  raise SystemExit('ERROR: --year must be one of: UL18, UL17, UL16')

# process choice of modules
if not args.twoprongExtra:
  if args.twoprongSB == 'None':
    constructor = 'default'
  if args.twoprongSB == 'full':
    constructor = 'addLoose'
    twoprong_sideband = 'Isolation and Symmetry'
else:
  if args.twoprongSB == 'None':
    constructor = 'optionalTrack'
  if args.twoprongSB == 'full':
    constructor = 'optionalTrack_addLoose'
    twoprong_sideband = 'Isolation and Symmetry'
if args.photonSB == 'None':
  phoconstructor = 'default'
if args.photonSB == 'full':
  phoconstructor = 'addLoose'
  photon_sideband = 'Full'
if args.selection == 'None':
  selection = 'default'
if args.selection == 'muon':
  selection = 'muon'
  selection_text = 'slimmedMuons >= 1'
if args.selection == 'photon':
  selection = 'photon'
  selection_text = 'slimmedPhotons >= 1'

# define max files
maxfiles = args.files
if args.files < 1: percentmax = True
else: percentmax = False

# determine schedd limit
if args.scheddLimit == -1:
  if site == 'hexcms': args.scheddLimit = 350
  if site == 'cmslpc': args.scheddLimit = 1500

# prepare prebuild area to send with job
if args.rebuild:
  print("Setting up src directory (inside ./"+cmssw_prebuild_area+") to ship with job")
  os.system('./' + helper_dir +'/'+ src_setup_script)
  print("\nFinished setting up directory to ship with job.\n")
if not args.rebuild and not os.path.isdir(cmssw_prebuild_area):
  raise SystemExit("ERROR: Prebuild area not prepared, use option --rebuild to create")

# check input
input_not_set = False
if re.match("(?:" + "/.*/.*/MINIAOD" + r")\Z", args.input) or \
   re.match("(?:" + "/.*/.*/MINIAODSIM" + r")\Z", args.input): args.input_dataset = True
if args.input_local == False and args.input_cmslpc == False and args.input_dataset == False:
  input_not_set = True
if input_not_set and site == "hexcms": args.input_local = True
if input_not_set and site == "cmslpc": args.input_cmslpc = True
input_files = [] # each entry a file location
s = args.input

# check proxy
if site == 'hexcms' and args.input_dataset:
  if args.proxy == '':
    subprocess.check_output("./"+helper_dir+"/"+hexcms_proxy_script, shell=True)
    proxy_path = ((subprocess.check_output("./"+helper_dir+"/"+hexcms_proxy_script, shell=True)).strip()).decode('utf-8')
  else:
    proxy_path = args.proxy
  if not os.path.isfile(proxy_path):
    raise SystemExit("ERROR: No grid proxy provided! Please use command voms-proxy-init -voms cms")
  os.system('cp '+proxy_path+' .')
  proxy_filename = os.path.basename(proxy_path)
  time_left = str(timedelta(seconds=int(subprocess.check_output("./"+helper_dir+"/"+hexcms_proxy_script_timeleft, shell=True))))
  if time_left == '0:00:00': raise SystemExit("ERROR: No time left on grid proxy! Renew with voms-proxy-init -voms cms")
if site == 'cmslpc':
  time_left = str(timedelta(seconds=int(subprocess.check_output("voms-proxy-info -timeleft", shell=True))))
  if time_left == '0:00:00': raise SystemExit("ERROR: No time left on grid proxy! Renew with voms-proxy-init -voms cms")

# input is .txt file
if s[len(s)-4:len(s)] == ".txt":
  with open(args.input) as f:
    totalfiles = len(f) 
    if percentmax: maxfiles = int(args.files * totalfiles)
    for line in f:
      input_files.append(line.strip())
      if len(input_files) == maxfiles: break

# input is local
elif args.input_local:
  if os.path.isfile(args.input):
    input_files.append(os.path.abspath(args.input))
  if os.path.isdir(args.input):
    if args.input[len(args.input)-1] == '/': args.input = args.input[0:len(args.input)-1]
    cmd = 'ls -1 {}/*'.format(args.input)
    output = subprocess.check_output(cmd, shell=True).decode('utf-8')
    output = output.split('\n')
    for line in output:
      if not line.find(".root") == -1:
        input_files.append(os.path.abspath(line))
        if len(input_files) == maxfiles: break

# input is eos area on cmslc
elif args.input_cmslpc:
  if s[len(s)-5:len(s)] == ".root":
    input_files.append(args.input)
  else:
    list_of_files = subprocess.getoutput("xrdfs root://cmseos.fnal.gov ls " + args.input)
    list_of_files = list_of_files.split('\n')
    totalfiles = len(list_of_files) 
    if percentmax: maxfiles = int(args.files * totalfiles)
    for line in list_of_files:
      input_files.append(line)
      if len(input_files) == maxfiles: break

# input is dataset name
elif args.input_dataset:
  dataset_name = args.input
  if not dm.isCached(dataset_name, dataset_cache): dm.process(dataset_name, dataset_cache)
  input_files = dm.getFiles(dataset_name, dataset_cache, args.files)
else:
  raise SystemExit('ERROR: Checking input failed! Could not determine input type.')

# finish checking input
if len(input_files)==0:
  raise SystemExit('ERROR: No input files found! Check input argument.')
example_inputfile = str(input_files[0].strip())
ex_in = example_inputfile
if args.verbose:
  print("Input files:")
  for input_file in input_files:
    print('  '+input_file.strip())

# test input
if args.input_cmslpc:
  ret = os.system('xrdfs root://cmseos.fnal.gov/ ls ' + input_files[0] + ' > /dev/null')
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

# get git hash/tag
tag_info_frontend = subprocess.getoutput("git describe --tags --long")
tag_info_backend = subprocess.getoutput("cd {}/*/src/; git describe --tags --long".format(cmssw_prebuild_area))
tag_info_frontend = tag_info_frontend.split('-')
tag_info_backend = tag_info_backend.split('-')
f_tag = tag_info_frontend[0]
f_commits = tag_info_frontend[1]
f_hash = tag_info_frontend[2]
b_tag = tag_info_backend[0]
b_commits = tag_info_backend[1]
b_hash = tag_info_backend[2]
f_ver_string = f_tag+" +"+f_commits+" "+f_hash
b_ver_string = b_tag+" +"+b_commits+" "+b_hash
f_dir_string = 'f'+f_tag.replace('.','p')+"-"+f_commits+"-"+f_hash[-4:]
b_dir_string = 'b'+b_tag.replace('.','p')+"-"+b_commits+"-"+b_hash[-4:]

# create output area
base = args.output
if base[-1] == '/': base = base[:-1]
timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M")
output_path = base+'/'+timestamp+'/'+f_dir_string+"_"+b_dir_string
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

# splitting
num_total_files = len(input_files)
if args.numJobs==None and args.filesPerJob==None: args.filesPerJob = 1
if args.filesPerJob == None:
  num_total_jobs = args.numJobs
  num_files_per_job = math.ceil(num_total_files / float(num_total_jobs))
  N = int(num_files_per_job)
else:
  N = args.filesPerJob
input_filenames = [] # each entry a filename, and the file is a txt file of input filenames one per line
if args.useLFN: ext = '.txt' # file location taken raw
else: ext = '.dat' # file location to be parsed by cmssw cfg file
for count,set_of_lines in enumerate(grouper(input_files, N, '')):
  with open(input_file_filename_base+'_'+str(count)+ext, 'wt') as fi:
    if len(set_of_lines) == 0: continue
    if set_of_lines[0] == '': continue
    for line in set_of_lines:
      if line == '': continue
      fi.write(line.strip()+'\n')
    input_filenames.append(os.path.basename(fi.name))
TOTAL_JOBS = len(input_filenames)

# create tranches
TRANCHE_LIMIT = args.trancheMax
procs = range(len(input_filenames))
if TOTAL_JOBS > TRANCHE_LIMIT:
  infile_tranches = [input_filenames[i:i + TRANCHE_LIMIT] for i in range(0, len(input_filenames), TRANCHE_LIMIT)]
  proc_tranches = [procs[i:i + TRANCHE_LIMIT] for i in range(0, len(procs), TRANCHE_LIMIT)]
else:
  infile_tranches = [input_filenames]
  proc_tranches = [procs]
if args.verbose:
  for i in range(len(infile_tranches)):
    print("  Tranche "+str(i+1))
    print("  procs: "+str(list(proc_tranches[i])))

# prepare unpacker script
template_filename = helper_dir+"/template_"+unpacker_filename
new_unpacker_filename = unpacker_filename
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
  else: to_replace['__copycommand__'] = 'xrdcp --nopbar'
use_template_to_replace(template_filename, new_unpacker_filename, to_replace)

# prepare stageout script
template_filename = helper_dir+"/template_"+stageout_filename
new_stageout_filename = stageout_filename
to_replace = {}
to_replace['__finalfile__'] = finalfile_filename
to_replace['__outputlocation__'] = output_path
if args.output_local:
  to_replace['__redirector__'] = ''
  to_replace['__copycommand__'] = 'cp'
if args.output_cmslpc:
  to_replace['__redirector__'] = 'root://cmseos.fnal.gov/'
  to_replace['__copycommand__'] = 'xrdcp --nopbar'
use_template_to_replace(template_filename, new_stageout_filename, to_replace)

# define submit file
subs = []
for i in range(len(infile_tranches)):
  if len(infile_tranches)==1: suffix = ''
  else: suffix = '_tranche'+str(i+1)
  if args.test: job_dir = 'TestJob_' + args.dir + suffix
  else: job_dir = 'Job_' + args.dir + suffix
  sub = htcondor.Submit()
  sub['executable'] = helper_dir+'/'+executable if not args.noPayload else helper_dir+'/'+executable_fast
  sub['arguments'] = unpacker_filename+" "+stageout_filename+" $(GLOBAL_PROC) "+datamc+" "+args.year
  if args.lumiMask is None:
    sub['arguments'] += " None"
  else:
    sub['arguments'] += " "+os.path.basename(args.lumiMask)
  sub['arguments'] += " "+constructor+" "+phoconstructor+" "+selection
  sub['should_transfer_files'] = 'YES'
  sub['+JobFlavor'] = 'longlunch'
  sub['Notification'] = 'Never'
  if site == 'cmslpc': sub['use_x509userproxy'] = 'true'
  if site == 'hexcms' and args.input_dataset: sub['x509userproxy'] = os.path.basename(proxy_path)
  sub['transfer_input_files'] = \
    job_dir+'/'+unpacker_filename + ", " + \
    job_dir+'/'+stageout_filename + ", " + \
    job_dir+'/infiles/'+input_file_filename_base+'_$(GLOBAL_PROC).dat' + ", " + \
    cmssw_prebuild_area+'/CMSSW_10_6_20/src/PhysicsTools' + ", " + \
    cmssw_prebuild_area+'/CMSSW_10_6_20/src/CommonTools'
  if not args.lumiMask is None:
    sub['transfer_input_files'] += ", "+args.lumiMask
  sub['transfer_output_files'] = '""'
  sub['initialdir'] = ''
  sub['JobBatchName'] = args.dir if args.batch is None else args.batch
  sub['output'] = job_dir+'/stdout/$(Cluster)_$(Process)_out.txt'
  if args.noErr:
    sub['error'] = '/dev/null'
  else:
    sub['error'] = job_dir+'/stdout/$(Cluster)_$(Process)_out.txt'
  sub['log'] = job_dir+'/log_$(Cluster).txt'
  if not args.scheddLimit==-1: sub['max_materialize'] = str(args.scheddLimit)
  subs.append(sub)

# make job directory
for i in range(len(infile_tranches)):
  if len(infile_tranches)==1: suffix = ''
  else: suffix = '_tranche'+str(i+1)
  if args.test: job_dir = 'TestJob_' + args.dir + suffix
  else: job_dir = 'Job_' + args.dir + suffix
  if args.test: args.force = True
  if os.path.isdir("./"+job_dir) and not args.force:
    raise SystemExit("ERROR: Directory " + job_dir + " already exists. Use option -f to overwrite")
  if os.path.isdir("./"+job_dir) and args.force:
    os.system('rm -rf ./' + job_dir)
  os.system('mkdir ' + job_dir)
  os.system('mkdir ' + job_dir + '/infiles')
  os.system('mkdir ' + job_dir + '/stdout')

# copy files to job directory
for i in range(len(infile_tranches)):
  if len(infile_tranches)==1: suffix = ''
  else: suffix = '_tranche'+str(i+1)
  if args.test: job_dir = 'TestJob_' + args.dir + suffix
  else: job_dir = 'Job_' + args.dir + suffix
  for filename in infile_tranches[i]:
    os.system('mv ' + filename + ' ' + job_dir + '/infiles/')
  os.system('cp ' + new_unpacker_filename + ' ' + job_dir)
  os.system('cp ' + new_stageout_filename + ' ' + job_dir)
  if not args.noPayload: os.system('cp ' + helper_dir +'/'+ executable + ' ' + job_dir)
  else: os.system('cp ' + helper_dir +'/'+ executable_fast + ' ' + job_dir)
  command = ''
  for a in sys.argv:
    command += a + ' '
  with open(submit_file_filename, 'wt') as f:
    f.write(subs[i].__str__())
    f.write('\n# Command:\n#'+command)
  os.system('mv ' + submit_file_filename + ' ' + job_dir)
os.system('rm ' + new_unpacker_filename)
os.system('rm ' + new_stageout_filename)


# print summary
if args.output_local: o_assume = 'local'
if args.output_cmslpc: o_assume = 'cmslpc eos'
if args.input_local: i_assume = 'local'
if args.input_cmslpc: i_assume = 'cmslpc eos'
if args.input_dataset: i_assume = 'official dataset'
print("Summary")
print("-------")
for i in range(len(infile_tranches)):
  if len(infile_tranches)==1: suffix = ''
  else: suffix = '_tranche'+str(i+1)
  if args.test: job_dir = 'TestJob_' + args.dir + suffix
  else: job_dir = 'Job_' + args.dir + suffix
  print("Job Directory       :", job_dir)
print("Job Batch Name      :", args.dir if args.batch is None else args.batch)
print("Job Specification   :", args.year +" "+datamc.upper())
if not args.twoprongSB=='None':
  print("Twoprong Sideband   : " + twoprong_sideband)
if args.twoprongExtra:
  print("Object Modification : " + "TwoProng Optional Extra Track")
if not args.photonSB=='None':
  print("Photon Sideband     : " + photon_sideband)
if not args.selection=='None':
  print("Preselection        : " + selection_text)
print("Total Jobs          :", str(TOTAL_JOBS))
print("Total Files         :", str(num_total_files))
print("Files/Job (approx)  :", str(N))
print("Input               : " + i_assume)
if len(input_files)>1: print("Example Input File  : " + ((ex_in[:88] + '..') if len(ex_in) > 90 else ex_in))
else : print("Input File          : " + ((ex_in[:88] + '..') if len(ex_in) > 90 else ex_in))
print("Output              : " + o_assume)
print("Output Directory    :", output_path)
if not args.lumiMask is None:
  print("Lumi Mask           : " + os.path.basename(args.lumiMask))
#print("Schedd              :", schedd_ad["Name"])
if args.input_dataset: print("Grid Proxy          :", time_left + ' left')

# prompt user to double-check job summary
if args.test:
  print("Just a test, Exiting.")
  sys.exit()
while True:
  response = input("Please check summary. [Enter] to proceed with submission, q to quit: ")
  if response == 'q':
    print("Quitting.")
    for i in range(len(infile_tranches)):
      if len(infile_tranches)==1: suffix = ''
      else: suffix = '_tranche'+str(i+1)
      if args.test: job_dir = 'TestJob_' + args.dir + suffix
      else: job_dir = 'Job_' + args.dir + suffix
      os.system('rm -rf '+job_dir)
    sys.exit()
  elif response == '': break
  else: pass

# get the schedd
coll = htcondor.Collector()
sched_query = coll.query(htcondor.AdTypes.Schedd, projection=["Name", "MyAddress"])
if site == 'hexcms': schedd_ad = coll.locate(htcondor.DaemonTypes.Schedd)
if site == 'cmslpc': schedd_ad = sched_query[0]
schedd = htcondor.Schedd(schedd_ad)

# submit
cluster_ids = []
first_procs = []
for i, tranche in enumerate(infile_tranches):
  total_procs = len(tranche)
  procs_list = proc_tranches[i]
  if len(infile_tranches)>1: print('  Submitting Tranche', i+1, ': procs', total_procs)
  if args.verbose and len(infile_tranches)>1: print('  '+str([e for e in procs_list]))
  iterator = subs[i].itemdata("queue 1 GLOBAL_PROC in "+", ".join(repr(e) for e in procs_list))
  result = schedd.submit(subs[i],itemdata = iterator)
  cluster_id = result.cluster()
  cluster_ids.append(cluster_id)
  first_procs.append(procs_list[0])
  print('  ClusterID', cluster_id)

# prepare job_info.py file
for i in range(len(infile_tranches)):
  if len(infile_tranches)==1: suffix = ''
  else: suffix = '_tranche'+str(i+1)
  if args.test: job_dir = 'TestJob_' + args.dir + suffix
  else: job_dir = 'Job_' + args.dir + suffix
  template_filename = helper_dir+"/template_"+jobinfo_filename
  replaced_filename = jobinfo_filename
  to_replace = {}
  to_replace['__frameworkversion__'] = str(f_ver_string)
  to_replace['__cluster__'] = str(cluster_ids[i])
  to_replace['__queue__'] = str(len(infile_tranches[i]))
  to_replace['__firstproc__'] = str(first_procs[i])
  to_replace['__schedd__'] = schedd_ad["Name"]
  to_replace['__output__'] = output_path
  use_template_to_replace(template_filename, replaced_filename, to_replace)
  os.system('mv ' + replaced_filename + ' ' + job_dir)
