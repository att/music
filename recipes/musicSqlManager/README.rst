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
The Music Sql Manager
==================================

An important part of ATT’s drive towards virtualization and ECOMP is multi-site resiliency for NFV
applications. Traditionally, most of these applications have used databases to maintain their data,
obtaining ACID semantics for all their operations. However, ACID semantics can be prohibitively
expensive if the state or data has to be replicated across multiple sites, due to both network
partitions and WAN latencies. On the other spectrum of data storage, eventually-consisent
data-stores like Cassandra  and Mongo-DB that forego ACID semantics for weaker consistency
properties are gaining in popularity because of their scalability and strong support for multi-site
deployments. In this work, we bridge this gap by a design in which applications write locally (or
within a datacenter) to a SQL cluster which is backed up by a multi-site MUSIC deployment which
captures every read and write that goes through the SQL database through database triggers. By doing
so, applications are guaranteed transactional semantics withing a site while they can chose between
eventually consistent and strongly consistent semantics across sites. 
