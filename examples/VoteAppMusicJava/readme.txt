/*
 * 
This licence applies to all files in this repository unless otherwise specifically
stated inside of the file. 

 ---------------------------------------------------------------------------
   Copyright (c) 2016 AT&T Intellectual Property

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at:

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ---------------------------------------------------------------------------

 */
This is a voting app that illustrates the features of Music. It is a simple app that creates a
keyspace and table within the data store that contains the vote count of candidates. The main file
to check is the VotingApp.java file. Currently the setting is for local for which we need to run a
local version of cassandra and zookeeper. To use it for the multi site version, uncomment the code
corresponding to obtaining the IPs of the multisite rest end points in Util.java. To use it in
multisite mode, in the createVotingKeyspace function change the replication factor to 3. 
