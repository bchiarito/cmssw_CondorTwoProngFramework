import os
import sys

# constants
filename = "__finalfile__"
output_location = "__outputlocation__"
redirector = "__redirector__"
copy_command = "__copycommand__"

print "Stageout: command:", copy_command + " " + filename + " " + redirector + output_location + "/" + filename.replace('.root', '')+'_'+str(sys.argv[1])+".root"
os.system(copy_command + " " + filename + " " + redirector + output_location + "/" + filename.replace('.root', '')+'_'+str(sys.argv[1])+".root")
