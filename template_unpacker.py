import sys
import os

# constants
input_file_filename = '__inputfilefilenamebase__' + '_' + sys.argv[1] + '.dat'
copy_command = '__copycommand__'
redirector = "__redirector__"

with open(input_file_filename) as f:
  for line in f:
    os.system(copy_command + " " + redirector + line.strip() + " .")
