# usage: ./restartHalIfDead.sh <hal id #> <Optional: "passive" or "p" or "-p"> 

if [ "$#" -lt 1 ]; then
    echo "Invalid number of arguments. Usage: "
    exit 1
fi

passive=""
if [[ "$#" -ge 2 && ${2//[-]} == p* ]]; then
    passive="-p"
    echo "Start HAL in passive mode"
fi
 
exit 0

dir=$PWD
ps aux > $dir/HalLog$1.out
halId=`grep "hal.jar $1" $dir/HalLog$1.out | awk '{ print $2 }'`
#echo $halId
if [ -z "${halId}" ]; then
#		echo hal dead
    java -jar $dir/hal.jar --id $1 --config $dir > $dir/hal$1.out & 
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
