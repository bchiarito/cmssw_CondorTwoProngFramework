import sys
import os

# constants
input_file_filename = '__inputfilefilenamebase__' + '_' + sys.argv[1] + '.dat'

with open(input_file_filename) as f:
  for line in f:
    os.system('cp ' + line.strip() + ' .')
