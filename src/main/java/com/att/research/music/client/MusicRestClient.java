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
package com.att.research.music.client;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import com.att.research.music.datastore.jsonobjects.JsonDelete;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.main.MusicUtil;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MusicRestClient {
	String[] musicNodes; 
	public MusicRestClient(String[] musicNodes){
		this.musicNodes = musicNodes;
	}
		
	public MusicRestClient(String oneMusicNode){
		musicNodes = new String[1];
		this.musicNodes[0] = oneMusicNode;
	}
	
	public String getMusicNodeURL(){
		String musicurl = "http://"+getMusicNodeIp()+":8080/MUSIC/rest";
		System.out.println(musicurl);
		return musicurl;
	}

	private String getMusicNodeIp(){
		Random r = new Random();
		int index = r.nextInt(musicNodes.length);	
		return musicNodes[index];
	}

	public void createKeyspace(String keyspaceName){
		System.out.println(keyspaceName);
		Map<String,Object> replicationInfo = new HashMap<String, Object>();
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", 1);
		String durabilityOfWrites="false";
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");
		com.att.research.music.datastore.jsonobjects.JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);
		jsonKp.setDurabilityOfWrites(durabilityOfWrites);
		jsonKp.setReplicationInfo(replicationInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	public void createStringMapTable(String keyspaceName, String tableName,Map<String,String> fields){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonTable jtab = new JsonTable();
		jtab.setFields(fields);
		jtab.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jtab);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}
	public void checkMusicVersion(){
		Client client = Client.create();

		WebResource webResource = client
				.resource(getMusicNodeURL()+"/version");

		ClientResponse response = webResource.accept("text/plain")
				.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

	//	System.out.println("Output from Server .... \n");
		System.out.println(output);

	}

	public  void createRow(String keyspaceName, String tableName,Map<String, Object> values){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		String url =getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows";
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url+"values:"+values);


	}
	
	private void basicUpdateRow(String keyspaceName, String tableName, String primaryKeyName, String primaryKeyValue, Map<String, Object> values, Map<String,String> consistencyInfo){
		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url =getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+primaryKeyName+"="+primaryKeyValue;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url+" values:"+values);

	}

	public  void updateEntry(String keyspaceName, String tableName, String primaryKeyName, String primaryKeyValue, Map<String, Object> values){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");
		basicUpdateRow(keyspaceName, tableName, primaryKeyName, primaryKeyValue, values, consistencyInfo);
	}

	public  void deleteEntry(String keyspaceName, String tableName, String primaryKeyName, String primaryKeyValue){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonDelete jDel = new JsonDelete();
		jDel.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url =getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+primaryKeyName+"="+primaryKeyValue;
		//System.out.println(url);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").delete(ClientResponse.class, jDel);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);

	}

	public  Map<String,Object> readRow(String keyspaceName, String tableName, String primaryKeyName, String primaryKeyValue){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url =getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+primaryKeyName+"="+primaryKeyValue;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}

	public  Map<String,Object> readAllRows(String keyspaceName, String tableName){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url =getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows";		
		WebResource webResource = client.resource(url);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}
	
	
	public void dropKeySpace(String keyspaceName){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = webResource.type("application/json")
				.delete(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}

	public  String createLock(String primaryKeyValue){
		Client client = Client.create();
		String msg =getMusicNodeURL()+"/locks/create/"+primaryKeyValue;
		WebResource webResource = client.resource(msg);

		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+msg);
		}

		String output = response.getEntity(String.class);

		return output;
	}

	public  boolean acquireLock(String lockId){
		Client client = Client.create();
		String msg =getMusicNodeURL()+"/locks/acquire/"+lockId;
		WebResource webResource = client.resource(msg);


		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+msg);
		}

		String output = response.getEntity(String.class);
		Boolean status = Boolean.parseBoolean(output);
		return status;
	}

	public  void releaseLock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(getMusicNodeURL()+"/locks/release/"+lockId);

		ClientResponse response = webResource.delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}

	
	public  void updateRowAtomically(String keyspaceName, String tableName, String primaryKeyName, String primaryKeyValue, Map<String, Object> values){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */
		//System.out.println("trying to acquire lock!");

		String lockName = keyspaceName+"."+tableName+"."+primaryKeyValue;
		String lockId = createLock(lockName);
		while(acquireLock(lockId) != true);
		
		//System.out.println("acquired lock!");

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");
		consistencyInfo.put("lockId", lockId);

		basicUpdateRow(keyspaceName, tableName, primaryKeyName, primaryKeyValue, values, consistencyInfo);

		//release lock now that the operation is done
		releaseLock(lockId);

	}
	
	public String getMusicId(){
		
		Client client = Client.create();

		WebResource webResource = client
				.resource(getMusicNodeURL()+"/nodeId");
		if(MusicUtil.debug)System.out.println("the url sent to get the id:"+getMusicNodeURL()+"/nodeId");
		ClientResponse response = webResource.accept("text/plain")
				.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
		return output;
	}

	public  void deleteRowAtomically(String keyspaceName, String tableName, String primaryKeyName, String primaryKeyValue, Map<String, Object> values){
		
	}

	public void deleteLock(String lockName){
		Client client = Client.create();
		WebResource webResource = client.resource(getMusicNodeURL()+"/locks/delete/"+lockName);

		ClientResponse response = webResource.delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}
}
			
