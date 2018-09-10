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
package main;

import java.util.Map;


public class JsonKeySpace {
    private Map<String,Object> replicationInfo;
	private String durabilityOfWrites;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

	public Map<String, Object> getReplicationInfo() {
		return replicationInfo;
	}
	
	public void setReplicationInfo(Map<String, Object> replicationInfo) {
		this.replicationInfo = replicationInfo;
	}

	public String getDurabilityOfWrites() {
		return durabilityOfWrites;
	}
	public void setDurabilityOfWrites(String durabilityOfWrites) {
		this.durabilityOfWrites = durabilityOfWrites;
	}
		
	

}
