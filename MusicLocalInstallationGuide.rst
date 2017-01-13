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

Instructions
============
1. If you are using a VM make sure it has at least 8 GB of RAM (I think it
works even with 4 GB, but with 2 GB it did give issues).
2. Ensure you have java jdk 7 or 8 working on your machine.
3. Download apache cassandra 3.0 and follow these instructions till and
including Step 4. By the end of this you should have Cassandra working.
4. Download Zookeeper 3.4.6 from and follow these instructions pertaining to
the standalone operation. By the end of this you should have Zookeeper
working.
5. Download Apache Tomcat 8 and follow these instructions for mac installation
(I have not used other OSes, but this is standard stuff).  Download the music
war file from here into the webapps folder of the tomcat installation.
6. Download the client app for Music from here , then use a java editor to
import the maven project, VoteAppForMUSIC, and then run the file VotingApp.
The expected output should be pretty easy to understand just by looking at the
file VotingApp.java.

