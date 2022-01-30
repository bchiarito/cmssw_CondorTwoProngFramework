import sys
import os

# constants
input_file_filename = '__inputfilefilenamebase__' + '_' + sys.argv[1] + '.dat'
copy_command = '__copycommand__'
redirector = "__redirector__"
RETRIES = 10

with open(input_file_filename) as f:
  for line in f:
    os.system(copy_command + " " + redirector + line.strip() + " .")
    # retry logic
    i = line.rfind('/')
    filename = (line[i+1:len(line)]).strip()
    ls = os.listdir(".")
    counter = 0
    while not filename in ls:
      if counter == RETRIES:
        print "\nUnpacker: Reached Retry Limit! Will not try again!\n"
        break
      print "\nUnpacker: Copy failed, file not found! Will retry ...\n"
      os.system(copy_command + " " + redirector + line.strip() + " .")
      counter += 1      
