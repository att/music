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
import java.util.ArrayList;
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

public class JerseyClientGet {

	public static void createVotingKeyspace(){
		Map<String,Object> replicationInfo = new HashMap<String, Object>();
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", 3);
		String durabilityOfWrites="true";
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
				.resource(MusicUtil.getMusicNodeURL()+"/keyspaces/VotingApp");

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299)
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	public static void createVotingTable(){
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
				.resource(MusicUtil.getMusicNodeURL()+"/keyspaces/VotingApp/tables/votecount");

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jtab);

		if (response.getStatus() < 200 || response.getStatus() > 299)
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}
	private static void checkMusicVersion(){
		Client client = Client.create();

		WebResource webResource = client
				.resource(MusicUtil.getMusicNodeURL()+"/version");

		ClientResponse response = webResource.accept("text/plain")
				.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

	//	System.out.println("Output from Server .... \n");
	//	System.out.println(output);

	}

	private static void createEntryForCandidate(String candidateName){
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

		WebResource webResource = client
				.resource(MusicUtil.getMusicNodeURL()+"/keyspaces/VotingApp/tables/votecount/rows");

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299)
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());


	}

	private static String createLock(String lockName){
		Client client = Client.create();
		WebResource webResource = client.resource(MusicUtil.getMusicNodeURL()+"/locks/create/"+lockName);

		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

//		System.out.println("Server response .... \n");
//		System.out.println(output);
		return output;
	}

	private static boolean acquireLock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(MusicUtil.getMusicNodeURL()+"/locks/acquire/"+lockId);


		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
		Boolean status = Boolean.parseBoolean(output);
	//	System.out.println("Server response .... \n");
	//	System.out.println(output);
		return status;
	}

	private static void unlock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(MusicUtil.getMusicNodeURL()+"/locks/release/"+lockId);

		ClientResponse response = webResource.delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}

	private static void updateVoteCountAtomically(String candidateName,int count){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */
		String lockName = "votingapp.votecount."+candidateName;
		String lockId = createLock(lockName);
		while(acquireLock(lockId) != true);

		//update candidate entry if you have the lock
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",count);

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

		WebResource webResource = client
				.resource(MusicUtil.getMusicNodeURL()+"/keyspaces/VotingApp/tables/votecount/rows?name="+candidateName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299)
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

		//release lock now that the operation is done
		unlock(lockId);

	}
	
	private static Map<String,Object> readVoteCountForCandidate(String candidateName){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		WebResource webResource = client
				.resource(MusicUtil.getMusicNodeURL()+"/keyspaces/VotingApp/tables/votecount/rows?name="+candidateName);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299)
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

		@SuppressWarnings("unchecked")
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}

	private static Map<String,Object> readAllVotes(){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		WebResource webResource = client
				.resource(MusicUtil.getMusicNodeURL()+"/keyspaces/VotingApp/tables/votecount/rows");

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299)
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

		@SuppressWarnings("unchecked")
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}
	
	public static void main(String[] args) {
		try {

			checkMusicVersion();
			createVotingKeyspace();
			createVotingTable();

			//the next few lines just create an entry in the voting table for all these candidates with vote count as 0
			createEntryForCandidate("Trump");
			createEntryForCandidate("Bush");
			createEntryForCandidate("Jebb");
			createEntryForCandidate("Clinton");

			//update the count atomically
			updateVoteCountAtomically("Trump",5);
			updateVoteCountAtomically("Bush",7);
			updateVoteCountAtomically("Clinton",8);
			updateVoteCountAtomically("Jebb",2);
			
			//read votecount 		
			System.out.println(readVoteCountForCandidate("Trump"));
			System.out.println(readAllVotes());
			
			//cleanup
			

		} catch (Exception e) {

			e.printStackTrace();

		}

	}
}