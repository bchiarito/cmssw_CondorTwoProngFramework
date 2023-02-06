#!/usr/bin/env python3
import sys
import socket
import argparse

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
args = parser.parse_args()

# ensure: source has no ending slash, destination has ending slash
if args.source[len(args.source)-1] == '/': args.source = args.source[:-1]
if not args.dest[len(args.dest)-1] == '/': args.dest = args.dest + '/'

# define submit file
sub = htcondor.Submit()
sub['executable'] = 'helper/move.sh'
sub['arguments'] = args.source + ' ' + args.dest
sub['+JobFlavor'] = 'longlunch'
sub['Notification'] = 'Never'
sub['use_x509userproxy'] = 'true'
sub['output'] = '$(Cluster)_$(Process)_out.txt'
sub['error'] = '$(Cluster)_$(Process)_out.txt'
sub['log'] = 'log_$(Cluster).txt'

collector = htcondor.Collector()
coll_query = collector.query(htcondor.AdTypes.Schedd, \
constraint='FERMIHTC_DRAIN_LPCSCHEDD=?=FALSE && FERMIHTC_SCHEDD_TYPE=?="CMSLPC"',
projection=["Name", "MyAddress"]
)
schedd_ad = coll_query[1]
schedd = htcondor.Schedd(schedd_ad)

# submit
result = schedd.submit(sub)
print(result.cluster())
