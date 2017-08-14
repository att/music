dir=$1 #give full path
ps aux > $dir/HalLog.out
halId=`grep "hal.jar" $dir/HalLog.out | awk '{ print $2 }'`
sudo kill -9 $halId
rm $dir/HalLog.out

ps aux > $dir/appLog.out
vappId=`grep "vote.jar" $dir/appLog.out | awk '{ print $2 }'`
sudo kill -9 $vappId 
rm $dir/appLog.out

rm *.out
