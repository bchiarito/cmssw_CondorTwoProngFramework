import os
import hashlib
import subprocess

master_filename = 'index.txt'
info_filename = 'info.txt'

def convertToDir(dataset):
  return hashlib.md5(dataset).hexdigest()

def convertToString(dataset):
  return dataset.replace('/', '__')

def isCached(dataset, dirname):
  if not os.path.isdir(dirname):
    raise Exception('Dataset Management: Not a valid cache directory!')
  d = convertToDir(dataset)
  if os.path.isdir(dirname+'/'+d) and os.path.isfile(dirname+'/'+d+'/'+master_filename): return True
  else: return False

def process(dataset, dirname):
  if isCached(dataset, dirname):
    raise Exception('Dataset Management: Trying to process dataset but dataset is already cached!')
  d = convertToDir(dataset)
  os.system('mkdir -p '+dirname+'/'+d)
  print "Dataset Management: Invoking DAS ..."
  files = subprocess.check_output('/cvmfs/cms.cern.ch/common/dasgoclient --query="file dataset='+dataset+'"', shell=True)
  print files
  print "Dataset Management: DAS query Successful."
  with open(dirname+'/'+d+'/'+master_filename, 'w') as f:
    f.write(files)
  with open(dirname+'/'+d+'/'+info_filename, 'w') as f:
    f.write('Dataset: '+dataset)
  
def getFiles(dataset, dirname, maxFiles):
  if not isCached(dataset, dirname):
    raise Exception('Dataset Management: Trying to get files but dataset is not cached!')
  d = convertToDir(dataset)
  files = []
  with open(dirname+'/'+d+'/'+master_filename, 'r') as f:
    for line in f:
      files.append(line)
      if len(files) == maxFiles: break
  return files
