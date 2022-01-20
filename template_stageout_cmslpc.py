import os
import sys

# constants
filename = "__finalfile__"
output_location = "__outputlocation__"

os.system('xrdcp --nobpar ' + filename + " root://cmseos.fnal.gov/" +
           output_location + "/" + filename.replace('.root', '')+'_'+str(sys.argv[1])+".root")
