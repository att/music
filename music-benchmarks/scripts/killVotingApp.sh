dir=/Users/bharathb/AttWork/Music/Rest-Music/examples/VoteAppMusicJava/scripts
ps aux > $dir/appLog$1.out
vappId=`grep "vote.jar $1" $dir/appLog$1.out | awk '{ print $2 }'`
sudo kill -9 $vappId 
rm $dir/appLog$1.out
