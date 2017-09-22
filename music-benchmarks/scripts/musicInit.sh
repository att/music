# Usage:
# First Populate the musicBm.properties file and provide the music/cassa/zk operation details. 
# The num_entries should be chosen to be sufficiently larger than the number of threads (say 10%). 

num_entries=$1
echo Initializing music..

java -jar ../lib/ext/musicJmeter.jar  musicBm.properties $num_entries
