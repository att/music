dir=$PWD
echo $dir
echo $2> $dir/modeOfCoreReplica$1.out
ps aux > $dir/appLog$1.out
vappId=`grep "vote.jar $1" $dir/appLog$1.out | awk '{ print $2 }'`
#echo $vappId
if [ -z "${vappId}" ]; then
    java -jar $dir/vote.jar $1 $dir> $dir/Output$1.out & 
fi
sleep 3
ps aux > $dir/appLog$1.out
vappId=`grep "vote.jar $1" $dir/appLog$1.out | awk '{ print $2 }'`
if [ -z "${vappId}" ]; then
    echo "NotRunning"
else 
		echo "Running"
fi
rm $dir/appLog$1.out
