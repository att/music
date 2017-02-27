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
package com.att.research.music.unittests;

import java.util.HashMap;
import java.util.Map;

import com.att.research.music.client.MusicRestClient;

public class TestRestMusic {

	public static void main(String[] args){
		String[] musicNodes = {"localhost"};

		//general set up 
		MusicRestClient tc = new MusicRestClient(musicNodes);
		String keyspaceName = "accord_key_space";
		String tableName = "resources";
		tc.createKeyspace(keyspaceName);
		
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("name", "text");
		fields.put("address", "Map<text,text>");
		fields.put("stakeholders", "Map<text,text>");
		fields.put("PRIMARY KEY", "(name)");

		tc.createStringMapTable(keyspaceName, tableName, fields);
		
		//inserts
		Map<String,Object> sg1Values = new HashMap<String,Object>();
	    sg1Values.put("name", "sg1");
	    
	    Map<String, String> address = new HashMap<String, String>();
	    address.put("ip", "10.10.4.5");
	    address.put("port", "2080");
	    sg1Values.put("address", address);
	    
	    Map<String, String> stakeholders = new HashMap<String, String>();
	    stakeholders.put("st_name", "db_tier");
	    stakeholders.put("st_address", "34.56");
	    sg1Values.put("stakeholders", stakeholders);
	    tc.createRow(keyspaceName, tableName, sg1Values);


		Map<String,Object> sg2Values = new HashMap<String,Object>();
	    sg2Values.put("name", "sg2");
	    
	    Map<String, String> sg2address = new HashMap<String, String>();
	    sg2address.put("ip", "20.32.4.5");
	    sg2address.put("port", "43380");
	    sg2Values.put("address", sg2address);
	    
	    Map<String, String> sg2Stakeholders = new HashMap<String, String>();
	    sg2Stakeholders.put("st_name", "app_tier");
	    sg2Stakeholders.put("st_address", "21.56");
	    sg2Values.put("stakeholders", sg2Stakeholders);
	 	 tc.createRow(keyspaceName, tableName, sg2Values);

	 	//reads

	   	System.out.println("row specific read:"+tc.readRow(keyspaceName, tableName,"name","sg1"));
	   	
	   	System.out.println("all rows read:"+tc.readAllRows(keyspaceName, tableName));
	   	
	   	//non locked eventually consistent updates
		Map<String,Object> updatedSg1Values = new HashMap<String,Object>();

	    Map<String, String> updatedStakeholders = new HashMap<String, String>();
	    updatedStakeholders.put("st_name", "database_tier");
	    updatedStakeholders.put("st_address", "14.890");
	    updatedSg1Values.put("stakeholders", updatedStakeholders);
	    
	    tc.updateEntry(keyspaceName, tableName, "name", "sg1", updatedSg1Values);
	    
	   	System.out.println("after the non-locked update:"+tc.readRow(keyspaceName, tableName,"name","sg1"));
	    
	    //locking stuff
		String lockName = keyspaceName+"."+tableName+"."+"sg1";
	    String lockId = tc.createLock(lockName);
	    
	    while(tc.acquireLock(lockId) == false);
	    
	    tc.releaseLock(lockId);
	        
	    //atomic updates where internally locks are called and released
		Map<String,Object> updatedSg2Values = new HashMap<String,Object>();

	    Map<String, String> updatedSg2Address = new HashMap<String, String>();
	    updatedSg2Address.put("ip", "192.45");
	    updatedSg2Address.put("port", "77780");
	    updatedSg2Values.put("address", updatedSg2Address);
	    
	    tc.updateRowAtomically(keyspaceName, tableName, "name", "sg2", updatedSg2Values);
	    
	   	System.out.println("after the critical update:"+tc.readRow(keyspaceName, tableName,"name","sg2"));
	   	
	   	//deletes
	   	tc.deleteEntry(keyspaceName, tableName,"name","sg2");
	   	
	   	tc.deleteLock(keyspaceName+"."+tableName+"."+"sg1");
	   	tc.deleteLock(keyspaceName+"."+tableName+"."+"sg2");
	   	
	   	tc.dropKeySpace(keyspaceName);

	}

}
