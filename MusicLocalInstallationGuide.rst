..
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

==================================
Local Installation Guide for Music
==================================

Prerequisites
=============
If you are using a VM make sure it has at least 8 GB of RAM (It may work with 4 GB, but with 2 GB it does give issues).

Instructions
============
1. Ensure you have java jdk 8 or above working on your machine.
2. Download apache cassandra 3.0 and follow these `instructions <http://cassandra.apache.org/doc/latest/getting_started/installing.html>`__ till and including Step 4. By the end of this you should have Cassandra working.
3. Download Zookeeper 3.4.6 from and follow these `instructions <http://cassandra.apache.org/download/>`__ pertaining to the standalone operation. By the end of this you should have Zookeeper working.
4. Download the latest Apache Tomcat and follow these `instructions <http://tecadmin.net/install-tomcat-9-on-ubuntu/>`__ (this is for version 9).  Build the music war file and place within the webapps folder of the tomcat installation.
5. Download the client app for Music from `here <https://github.com/att/music/tree/master/examples/VoteAppMusicJava>`__, then use a java editor to import the maven project, VoteAppForMUSIC, and then run the file VotingApp.  The expected output should be pretty easy to understand just by looking at the file VotingApp.java.


