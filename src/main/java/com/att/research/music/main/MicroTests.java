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
package com.att.research.music.main;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MicroTests {
	final String keyspaceName="shankarks";
	final String  musicurl = "http://"+MusicUtil.musicRestIp+":8080/MUSIC/rest/formal";
	final String userForGets = "shankarUserForGets";
	public MicroTests(){
		bootStrap();
	}
	
	private void createVotingKeyspace(){
		System.out.println(keyspaceName);
		Map<String,Object> replicationInfo = new HashMap<String, Object>();
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", 3);
		String durabilityOfWrites="false";
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");
		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);
		jsonKp.setDurabilityOfWrites(durabilityOfWrites);
		jsonKp.setReplicationInfo(replicationInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(musicurl+"/keyspaces/"+keyspaceName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	private void createVotingTable(){
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("name", "text");
		fields.put("count", "varint");
		fields.put("PRIMARY KEY", "(name)");


		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonTable jtab = new JsonTable();
		jtab.setFields(fields);
		jtab.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(musicurl+"/keyspaces/"+keyspaceName+"/tables/votecount");

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jtab);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	private  void createEntryForCandidate(String candidateName){
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("name",candidateName );
		values.put("count",0);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		String url = musicurl+"/keyspaces/"+keyspaceName+"/tables/votecount/rows";
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}

	private  String createLock(String lockName){
		Client client = Client.create();
		String msg = musicurl+"/locks/create/"+lockName;
		WebResource webResource = client.resource(msg);
		System.out.println(msg);

		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

		return output;
	}

	private  boolean acquireLock(String lockId){
		Client client = Client.create();
		String msg = musicurl+"/locks/acquire/"+lockId;
		System.out.println(msg);
		WebResource webResource = client.resource(msg);


		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
		Boolean status = Boolean.parseBoolean(output);
		System.out.println("Server response .... \n");
		System.out.println(output);
		return status;
	}

	private  void unlock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(musicurl+"/locks/release/"+lockId);

		ClientResponse response = webResource.delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}
	
	public String musicCriticalPutAndUpdate(String candidateName){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */
		createEntryForCandidate(candidateName);
		System.out.println("trying to acquire lock!");

		String lockName = keyspaceName+".votecount."+candidateName;
		String lockId = createLock(lockName);
		while(acquireLock(lockId) != true);
		
		System.out.println("acquired lock!");
		//update candidate entry if you have the lock
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",5);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");
		consistencyInfo.put("lockId", lockId);

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicurl+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

		//release lock now that the operation is done
		unlock(lockId);
		return "musicCriticalPutAndUpdate:"+url;

	}

	public String musicPutAndUpdate(String candidateName){
		createEntryForCandidate(candidateName);

		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",5);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicurl+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		return "musicPutAndUpdate:"+url;
	}

	public String musicGet(){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		String url = musicurl+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+userForGets;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return "musicGet:"+url;	
	}

	public String cassaQuorumPutAndUpdate(String candidateName){
		//http://135.197.226.98:8080/MUSIC/rest/formal/purecassa/keyspaces/shankarks/tables/employees/rows?emp_name=shankaruser1
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",5);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicurl+"/purecassa/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		return "cassaQuorumPutAndUpdate:"+url;

	}

	public String cassaPutAndUpdate(String candidateName){
		//http://135.197.226.98:8080/MUSIC/rest/formal/purecassa/keyspaces/shankarks/tables/employees/rows?emp_name=shankaruser1
		long start = System.currentTimeMillis();
		createEntryForCandidate(candidateName);

		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",5);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicurl+"/purecassa/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		WebResource webResource = client
				.resource(url);
		long end = System.currentTimeMillis();
		String time = (end-start)+"";
		
		start = System.currentTimeMillis();
		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);
		end = System.currentTimeMillis();
		String time2 = (end-start)+"";
		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		return "cassaPutAndUpdate:"+url;
	}

	public  String cassaGet(){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		String url = musicurl+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+userForGets;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return "cassaGet:"+url;	
	}
	
	private void zkCreate(String candidateName){
		//http://135.197.226.98:8080/MUSIC/rest/formal/purezk/shankarzknode
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(musicurl+"/purezk/"+candidateName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}
	
	public String zkPutAndUpdate(String candidateName){
		//http://135.197.226.99:8080/MUSIC/rest/formal/purezk/shankarzknode
		
		//CREATE IT FIRST
		zkCreate(candidateName);
		
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",5);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicurl+"/purezk/"+candidateName;
		System.out.println("in zk put:"+url);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	
		return "zkPutAndUpdate:"+url;
	}

	public String zkGet(){
		Client client = Client.create();
		String url = musicurl+"/purezk/"+userForGets;
		System.out.println("in zk get:"+url);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("text/plain")
				.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
		return "zkGet:"+url;
	}


	public void bootStrap(){
	//	createVotingKeyspace();
	//	createVotingTable();
	//	createEntryForCandidate(userForGets);
	//	zkPutAndUpdate(userForGets);
	}


}