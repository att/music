dir=$0PWD
echo $dir
echo $2> $dir/modeOfCoreReplica$1.out

#just dump all active process ids in a file to search for the core process id
ps aux > $dir/appLog$1.out

#Get the process id of the core app (in this case vote.jar)
vappId=`grep "vote.jar $1" $dir/appLog$1.out | awk '{ print $2 }'`

#check if the id is null 
if [ -z "${vappId}" ]; then #Core was not running
    java -jar $dir/vote.jar $1 $dir> $dir/Output$1.out & 
else #Core was already running
	exit 0
fi

#give it some time to start
sleep 3
ps aux > $dir/appLog$1.out
vappId=`grep "vote.jar $1" $dir/appLog$1.out | awk '{ print $2 }'`
rm $dir/appLog$1.out #just a temp file to write out the process id..

if [ -z "${vappId}" ]; then #Restart attempt failed
    exit 1 
else #Restart succeeded
		exit 2 
fi
