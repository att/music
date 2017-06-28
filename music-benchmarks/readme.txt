This is a voting app that illustrates the features of Music. It is a simple app that creates a
keyspace and table within the data store that contains the vote count of candidates. The main file
to check is the VotingApp.java file. Currently the setting is for local for which we need to run a
local version of cassandra and zookeeper. To use it for the multi site version, uncomment the code
corresponding to obtaining the IPs of the multisite rest end points in Util.java. To use it in
multisite mode, in the createVotingKeyspace function change the replication factor to 3. 
