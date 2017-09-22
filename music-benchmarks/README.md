
# MUSIC benchmarking through Apache Jmeter

Please follow these steps (in the sequence specified) to run a benchmark client that uses Jmeter to make mulit-threaded REST requests to MUSIC:

- Use an ubuntu/mac (tested on Ubuntu 14+ and MAC Yostemite) VM/machine and install Java 1.8 on it. 

- Download apache-jmeter-3.2 from here: http://jmeter.apache.org/download_jmeter.cgi and untar or unzip it in a preferred location. 

- Download the MUSIC related scripts from here: https://github.com/att/music/tree/master/music-benchmarks/scripts and put all of the files in apache-jmeter-3.2/bin/.

- Move musicJmeter.jar to apache-jmeter-3.2/lib/ext/ folder. This is the primary jar that specify the test that each jmeter thread will run. This can also be compiled from source from here: https://github.com/att/music/tree/master/music-benchmarks/

- Ensure MUSIC is running either on the local machine or across any set of machines with accessible public IPs. 

- Configure the musicBm.properties file with the exact music operation that needs to be tested and the location of the MUSIC Ips (localhost if MUSIC is running locally). The test will randomly send the REST requests across the MUSIC nodes. Here is a sample: 

			op.type=cassa_ev_put 
			parameter=-1
			ip.list=public ip of MUSIC node 0:public ip of MUSIC node 1:public ip of MUSIC node 2 
			
	The supported tests and the relevant parameters are the ones in the execute function here: https://github.com/att/music/blob/master/music-benchmarks/src/main/java/main/BmOperation.java

- 	Run musicInit.sh with the number of entries needed in MUSIC as a parameter. Ideally the jmeter threads should update independent rows. Hence, ensure that the number of entries is greater than or equal to that. this script will create a specific benchmarking keyspace, a table and populate the table with the specified number of entries. 

- The jmeter testplan is specified by musicTestplan.jmx. Most of it has already been configured according to requirements. Two options that might need to be changed are "LoopController.loops" that specifies the number of times each thread loops and "ThreadGroup.ramp\_time" that specifies the amount of time in which all of the threads need to be started. For more information on these choices, please refer to: http://jmeter.apache.org/usermanual/test_plan.html

-  Once the test plan is ready, run musicJmeterTest.sh with the number of threads and number of trials as input. The number of threads, tells jmeter how many threads to run in each experiment while the number of trials specifies how many times each experiment must be run. Based on the musicBm.properties shown here, if you run this script with parameters 10 and 2, then jmeter will send 10 parallel threads to MUSIC, each of which will send a REST request that will execute a cassandra eventual put through the MUSIC REST API. This experiment will be run twice. 

- The results are stored in apache-jmeter-3.2/bin/results folder with the naming convention op\_type.current\_date_time.trial\_number.txt. 



 