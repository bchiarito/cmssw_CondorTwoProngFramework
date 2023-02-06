#!/usr/bin/env python3
import sys
import os
import socket
import argparse
from datetime import datetime

# import condor modules
hostname = socket.gethostname()
if 'hexcms' in hostname: site = 'hexcms'
elif 'fnal.gov' in hostname: site = 'cmslpc'
elif 'cern.ch' in hostname: site = 'lxplus'
else: raise SystemExit('ERROR: Unrecognized site: not hexcms, cmslpc, or lxplus')
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
parser = argparse.ArgumentParser(description="", usage="./%(prog)s <source> <destination>\n\nredirector for cmslpc: root://cmseos.fnal.gov/\nredirector for hexcms: root://ruhex-osgce.rutgers.edu/")
parser.add_argument("source" ,help="path on eos including redirector")
parser.add_argument("dest", help="path on eos including redirector")
parser.add_argument("-d", "--dir", dest='jobdir', default='Job_move_'+datetime.now().strftime("%Y-%m-%d-%H-%M-%S"), help="jobdir name")
args = parser.parse_args()

os.system('mkdir '+args.jobdir)

# define submit file
sub = htcondor.Submit()
sub['executable'] = 'helper/move.sh'
sub['arguments'] = args.source + ' ' + args.dest
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
if site == 'cmslpc': sub['use_x509userproxy'] = 'true'
if site == 'hexcms': sub['x509userproxy'] = os.path.basename('x509up_u756')
sub['output'] = args.jobdir+'/$(Cluster)_$(Process)_out.txt'
sub['error'] = args.jobdir+'/$(Cluster)_$(Process)_out.txt'
sub['log'] = args.jobdir+'/log_$(Cluster).txt'

collector = htcondor.Collector()
if site == 'cmslpc':
  coll_query = collector.query(htcondor.AdTypes.Schedd, \
    constraint='FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"',
    projection=["Name", "MyAddress"]
  )
  schedd_ad = coll_query[1]
if site == 'hexcms':
  schedd_ad = collector.locate(htcondor.DaemonTypes.Schedd) 
schedd = htcondor.Schedd(schedd_ad)

# make directory
if site == 'hexcms':
  os.system('mkdir -p '+args.dest)

# submit
result = schedd.submit(sub)
print(result.cluster())
