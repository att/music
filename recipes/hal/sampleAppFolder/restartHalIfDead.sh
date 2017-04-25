dir=$PWD
ps aux > $dir/HalLog$1.out
halId=`grep "hal.jar $1" $dir/HalLog$1.out | awk '{ print $2 }'`
#echo $halId
if [ -z "${halId}" ]; then
#		echo hal dead
    java -jar $dir/hal.jar $1 $dir> $dir/hal$1.out & 
fi
sleep 3
ps aux > $dir/HalLog$1.out
halId=`grep "hal.jar $1" $dir/HalLog$1.out | awk '{ print $2 }'`
if [ -z "${halId}" ]; then
    echo "NotRunning"
else 
		echo $halId
fi
rm $dir/HalLog$1.out
