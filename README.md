This license applies to all files in this repository unless otherwise specifically
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

The complexity of replicated, multi-site
distributed applications brings forth the need for rich distribution coordination patterns to manage
these applications. We contend that, to build such patterns, it is necessary to tightly integrate
coordination primitives such as mutual exclusion and barriers with state-management in these
replicated systems. This is easier said than done, since coordination primitives typically need
strong consistency that may render them unavailable during partitions. On the other hand, the
relative ubiquity of network partitions and large WAN latencies in a multi-site setting dictate that
replicated state is usually maintained in an eventually consistent store. We address this conflict
by presenting a MUlti-SIte Coordination service (MUSIC), that combines a strongly consistent locking
service with an eventually consistent state store to provide abstractions that enable rich
distributed coordination on shared state, as and when required.

This is the repository for MUSIC.  The pom.xml corresponds to the rest-based version of MUSIC. 
