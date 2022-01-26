python wrapper script to submit condor jobs on the site where the script is run
runs on MiniAOD to produce Custom NanoAOD. NanoAOD which includes TwoProng objects

Works on the following sites
* hexcms
* cmslpc

Input may be located on
* local filesystem
* cmsplc eos area (i.e., /store/uster/... on cmslpc )
* dataset (i.e., /store/user not on cmslpc )

Output may be
* local filesystem
* cmslpc eos area of user running script

Currently only works with UL18 prescription

For instructions, run:
```
$ python condor_submit.py --help
```
