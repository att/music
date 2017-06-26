
# High Availability (HAL) Protocol for Active-Passive Systems 

Often we wish to deploy service replicas in an active-passive mode where there is only one active service and if that fails, one of the passives takes over as the new active. The trouble is, to implement this, the writer of the service has to worry about challenging distributed systems concepts like group membership, failure detection, leader election, split-brain problems and so on. HAL addresses this issue, by providing a library that services can simply configure and deploy as a companion daemon to their service replicas, that will handle all distributed systems issues highlighted above. 


<a name="local-install"> 

## Setup and Usage

</a>

- The starting point for HAL is that you wish to replicate a service on multiple servers/hosts/VMs (refereed to as a node) such that one of them is active and the others are passive at all times. 
- Ensure that MUSIC <a href="">https://github.com/att/music</a> is running across all these nodes as a cluster. 
-  Build hal and copy the resultant hal.jar into all the nodes along with config.json file and the restartHalIfDead.sh script (sample files provided in this repository under the sampleApp folder). 
-  Modify the config.json (same at all nodes). We explain the config.json through an example: 
		
		
		{
		    "appName":"votingAppBharath",
		    "ensure-active-0":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 0 active",
		    "ensure-active-1":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 1 active",
		    "ensure-active-2":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 2 active",
		    "ensure-active-3":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 3 active",
		    "ensure-passive-0":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 0 passive",
		    "ensure-passive-1":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 1 passive",
		    "ensure-passive-2":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 2 passive",
		    "ensure-passive-3":"/home/ubuntu/votingapp/ensureVotingAppRunning.sh 3 passive",
		    "restart-hal-0":"ssh -i /home/ubuntu/votingapp/bharath_cirrus101.pem ubuntu@135.197.240.156 /home/ubuntu/votingapp/restartHalIfDead.sh 0",
		    "restart-hal-1":"ssh -i /home/ubuntu/votingapp/bharath_cirrus101.pem ubuntu@135.197.240.158 /home/ubuntu/votingapp/restartHalIfDead.sh 1",
		    "restart-hal-2":"ssh -i /home/ubuntu/votingapp/bharath_bigsite.pem ubuntu@135.197.226.68 /home/ubuntu/votingapp/restartHalIfDead.sh 2",
		    "restart-hal-3":"ssh -i /home/ubuntu/votingapp/bharath_bigsite.pem ubuntu@135.197.226.49 /home/ubuntu/votingapp/restartHalIfDead.sh 3",
		    "timeout":"50000",
		    "noOfRetryAttempts":"3",
		    "replicaIdList":["0","1","2","3"]
		    "musicLocation":"localhost"
		} 
	The *appName*	 is simply the name chosen for the service. 
	
	The *ensure-active-i* and *ensure-passive-i* scripts need to be provided for 	all the service replicas, wherein the i corresponds to each of their ids. 	The ids must start from 0 with single increments. As seen in the example, 	the command within the string will be invoked by hal to run the servce in 	either active or passive mode. These scripts should return the linux exit 	code of 0 if they run successfully. 
	
	The *restart-hal-i* scripts are used by the hal daemons running along with 	each replica to restart each other. Since the hal daemons reside on 	different nodes, they will need ssh (and associated keys) to communicate 	with each other. 
	
	The *timeout* field decides the time after which one of the passive hals 	will take-over as leader after the current leader stops updating MUSIC with 	its health. The *noOfRetryAttempts* is used by hal to decide how many times 	it wants to try and start the local service replica in either active or 	passive mode (by calling the ensure- scripts). The *replicaIdList* is a 	comma separated list of the replica 	ids. Finally, the *musicLocation* should 	contain the public IP of the MUSIC 	node this hal daemon wants to talk to. 	Typically this is localhost if MUSIC is co-located on the same node as the 	hal deamon and service replica. 
	
		
- Once the config.json has been placed on all nodes in the same location as the hal.jar and the restartHalIfDead.sh, on one of the nodes (typically the one that you want as active), run the command:

		./restartHalIfDead.sh <node id>
		
	The hal protocol will not take over and start the service replicas in active-passive mode. The hal log can be found on each node i in the file hali.out. 