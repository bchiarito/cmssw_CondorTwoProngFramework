from __future__ import print_function
import os
import sys
import hashlib
import subprocess
import json

master_filename = 'index.txt'
info_filename = 'info.txt'

def convertToDir(dataset):
  u = dataset.find('_')
  h = dataset.find('-')
  s = dataset[1:].find('/')
  s += 1
  if u<=0 : u = 1000
  if h<=0 : h = 1000
  if s<=0 : s = 1000
  i = min(u,h,s)
  return str(dataset[:i])+"_"+str(hashlib.md5(dataset.encode('utf-8')).hexdigest())
  #return str(dataset[:i])+"_"+str(hashlib.md5(dataset).hexdigest())

def convertToString(dataset):
  return dataset.replace('/', '__')

def isCached(dataset, dirname):
  if not os.path.isdir(dirname):
    raise SystemExit('Dataset Management: Not a valid cache directory! Please create empty directory: ' + dirname)
  d = convertToDir(dataset)
  if os.path.isdir(dirname+'/'+d) and os.path.isfile(dirname+'/'+d+'/'+master_filename): return True
  else: return False

def process(dataset, dirname):
  if isCached(dataset, dirname):
    raise SystemExit('Dataset Management: Trying to process dataset but dataset is already cached!')
  d = convertToDir(dataset)
  print("Dataset Management: Invoking DAS ...")
  files = subprocess.check_output('/cvmfs/cms.cern.ch/common/dasgoclient --query="file dataset='+dataset+'"', shell=True).decode('utf-8')
  info = subprocess.check_output('/cvmfs/cms.cern.ch/common/dasgoclient --json --query="dataset='+dataset+'"', shell=True).decode('utf-8')
  parsed_info = json.loads(info)
  if not parsed_info[2]['das']['services'][0] == "dbs3:filesummaries":
    raise SystemExit('Dataset Management: Something wrong parsing dataset json query results! (sometimes just try again) \nDump:\n'+json.dumps(parsed_info,indent=2))
  parsed_info = parsed_info[2]['dataset'][0]
  space = float(parsed_info['size'])/1e12
  nfiles = int(parsed_info['nfiles'])
  nevents = int(parsed_info['nevents'])
  avg_events_files = int(float(nevents)/nfiles)
  avg_size_files = space*1000/nfiles
  print("Dataset Management: DAS query Successful.")
  os.system('mkdir -p '+dirname+'/'+d)
  with open(dirname+'/'+d+'/'+master_filename, 'wt') as f:
    f.write(files)
  with open(dirname+'/'+d+'/'+info_filename, 'wt') as f:
    f.write('Dataset: '+dataset)
    f.write('\nSize: '+str(round(space,1))+' TB')
    f.write('\nFiles: '+'{:,}'.format(nfiles))
    f.write('\nEvents: '+'{:,}'.format(nevents))
    f.write('\n{:,} events/file'.format(avg_events_files))
    f.write('\n{} GB/file'.format(round(avg_size_files,1)))
  
def getFiles(dataset, dirname, maxFiles):
  if not isCached(dataset, dirname):
    raise Exception('Dataset Management: Trying to get files but dataset is not cached!')
  d = convertToDir(dataset)
  files = []
  if maxFiles < 1:
    totalfiles = len(open(dirname+'/'+d+'/'+master_filename, 'rt').readlines())
    maxFiles = int(maxFiles * totalfiles)
  with open(dirname+'/'+d+'/'+master_filename, 'rt') as f:
    for line in f:
      files.append(line)
      if len(files) == maxFiles: break
  return files
