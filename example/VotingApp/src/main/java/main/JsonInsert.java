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

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
public class JsonInsert {
	
    private Map<String,Object> values;
    String ttl, timestamp;
	private Map<String,Object> row_specification;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

	public String getTtl() {
		return ttl;
	}
	public void setTtl(String ttl) {
		this.ttl = ttl;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, Object> getValues() {
		return values;
	}
	public void setValues(Map<String, Object> values) {
		this.values = values;
	}
    public Map<String, Object> getRow_specification() {
		return row_specification;
	}
	public void setRow_specification(Map<String, Object> row_specification) {
		this.row_specification = row_specification;
	}
}
