#!/bin/sh
#Usage:
# First Populate the musicBm.properties file and provide the music/cassa/zk operation details 
# Then run :
# ./musicJmeterTest.sh <num_threads> <num_trials>
#
# num_threads: The number of simultaneous jmeter threads to test the operation
# num_trials: The number of trials for which each experiment should be run, to get an average of
# results. 


function getProperty {
   PROP_KEY=$1
   PROP_VALUE=`cat musicBm.properties | grep "$PROP_KEY" | cut -d'=' -f2`
   echo $PROP_VALUE
}

num_threads=$1
num_trials=$2
op_type=$(getProperty "op.type")

echo num_threads = $num_threads
echo num_trials = $num_trials
echo operation_type = $op_type

current_date_time="`date +%d_%H_%M`";

#start running the trials

for (( i=1; i<=$num_trials; i++ ))
do  
	result_file_name=$op_type.$current_date_time.trial_$i.txt
  echo -----Trial $i started, results in file results/$result_file_name---
	./jmeter -Jusers=$num_threads -n -t musicTestplan.jmx -l results/$result_file_name
done
