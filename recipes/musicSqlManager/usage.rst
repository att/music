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

========================================================
Installation and Usage Guide for music sql manager recipe
========================================================
1. Install Cassandra on your cluster

2. Import the pom.xml into a maven project in an editor of your choice (like Eclipse)

3. Generate a jar file, say mdbc.jar corresponding to the configuration of the file
MusicSqlManager.java with all the required libraries

4. Configure the config.properties and the log4j.properties and place it in the same folder as the
jar 

5. Run the jar using java -jar mdbc.jar and you should see the log4j printouts for a basic test case
in which we create tables, insert, update, delete and select rows from it and finally a constant loop
that prints out all the public tables in this database. 
