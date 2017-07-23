#Usage:
# First Populate the music_bm.json file and provide the music/cassa/zk operation details 
# Then run :
# ./runJmeterTest.sh <num_threads> <num_trials>
#
# num_threads: The number of simultaneous jmeter threads to test the operation
# num_trials: The number of trials for which each experiment should be run, to get an average of
# results. 


num_threads=$1
num_trials=$2
op_type=$(head -n 1 music_bm.txt)


echo operation_type = $op_type
echo num_threads = $num_threads
echo num_trials = $num_trials
current_date_time="`date +%d_%H_%M`";
folder_name=$op_type
echo results stored in folder: results/$folder_name

#start running the trials

for (( i=1; i<=$num_trials; i++ ))
do  
	result_file_name=run_$current_date_time.trial_$i.txt
  echo -----Trial $i started, results in file results/$folder_name/$result_file_name---
	./jmeter -Jusers=$num_threads -n -t testplans/music.jmx -l results/$folder_name/$result_file_name
done
