# overview

python wrapper to submit condor jobs on the site where the script is run
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
## checking job status

An executable script ``condor_status.py`` is provided, but the following options can also be used if it fails for some reason or other
```
$ condor_wait -status <path/to/logfile>
$ eosls -lh </full/path/to/output/directory>
```
check condor priority with
```
$ condor_userprio -priority
```
## files/job and other numbers to use when running

mc: 100k events per subjob is good. 150k hits 2 day limit

signal: 50k events per subjob is good. 10k finishes in 5-6 hours, 100k finishes in 44 hrs

data: testing

## size of output

bkg mc nano: ~2.8 GB/mil

signal mc nano: ~4 GB/mil

data nano: testing
