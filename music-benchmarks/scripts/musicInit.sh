# Usage:
# First Populate the music_bm.txt file and provide the music/cassa/zk operation details. This script
# needs to ip address of MUSIC especially. The num_entries should be chosen to be sufficiently
# larger than the number of threads (say 10%). 

num_entries=$1
echo Initializing music..

java -jar ../lib/ext/music_jmeter.jar  music_bm.properties $num_entries
