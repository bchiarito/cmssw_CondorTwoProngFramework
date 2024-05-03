# overview

python wrapper to submit condor jobs on the site where the script is run
runs on MiniAOD to produce Custom NanoAOD. NanoAOD which includes TwoProng objects

Works on the following sites
* hexcms
* cmslpc

Input may be located on
* local filesystem
* cmsplc eos area (i.e., /store/user/... on cmslpc )
* dataset (i.e., /store/user not on cmslpc )

Output may be
* local filesystem
* cmslpc eos area of user running script

## setup and running 

To set up, do: 
```
git clone git@github.com:bchiarito/cmssw_CondorTwoProngFramework.git
cd cmssw_CondorTwoProngFramework
```

Before starting to process a new dataset, make sure you do ```git pull``` from inside cmssw_CondorTwoProngFramework/

Set up your grid proxy with ```voms-proxy-init --valid 192:00 -voms cms```

For instructions, run:
```
$ ./condor_submit.py --help
```

If you already did cmsenv in this shell session, you will see an error message saying something like ```ERROR: Could not import classad or htcondor.``` To unset, do: 
```eval `scram unsetenv -sh````

##Sample commands (3 May 2024)

Data: 
```
./condor_submit.py /SingleMuon/Run2017C-UL2017_MiniAODv2-v1/MINIAOD /store/user/lpcrutgers/sthayil/pseudoaxions/nano/singlemuonC_2017_03-24/ --data --twoprongSB full -y UL17 -l lumimasks/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt -d singlemuonC_2017_03-24 -v -f --jobsPerFile=3  --rebuild --twoprongExtra
```
Make sure that you're using --jobsPerFile=3 (or greater)for data, else the jobs won't finish within 48 hours and will be automatically terminated

MC:
```
./condor_submit.py /DYJetsToLL_M-50_TuneCP5_13TeV-madgraphMLM-pythia8/RunIISummer20UL17MiniAODv2-106X_mc2017_realistic_v9-v2/MINIAODSIM /store/user/lpcrutgers/sthayil/pseudoaxions/nano/dyjetstoll_2017_03-24/ --twoprongSB full -y UL17 -d dyjetstoll_2017_03-24 --mc -f -v --rebuild --twoprongExtra
```

## checking job status

An executable script ``condor_status.py`` is provided, but the following options can also be used
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
