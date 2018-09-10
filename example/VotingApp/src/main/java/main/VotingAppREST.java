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
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.WebResource.Builder;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class VotingAppREST {
	String keyspaceName;
	ArrayList<String> lockNames;
	MusicConnector musicHandle;
	private final String version="1.0.0";
	
	//UPDATE your onboarding information here
	String namespace = "votingapp";
	String userId = "abc123d";
	String password = "password";
	
	public VotingAppREST(String[] musicIps){
		lockNames = new ArrayList<String>();	
		musicHandle = new MusicConnector(musicIps);
		bootStrap();
	}
	
	/**
	 * Adds MUSIC's authentication headers into the webresource
	 * @param webResource
	 */
	private Builder addMusicHeaders(WebResource webResource) {
		Builder builder = webResource.accept(MediaType.APPLICATION_JSON).type(MediaType.APPLICATION_JSON);
		if (!namespace.equals("")) {
			builder.header("ns", namespace);
		}
		if (!userId.equals("")) {
			builder.header("userId", userId);
		}
		if (!password.equals("")) {
			builder.header("password", password);
		}
		
		return builder;
	}
	
	public void createVotingKeyspace(){
		keyspaceName = "VotingAppForMusic";
		System.out.println("Voting app version "+ version+" .....");
		Map<String,Object> replicationInfo = new HashMap<String, Object>();
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", 1);
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
				.resource(musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json")
				.type("application/json").post(ClientResponse.class, jsonKp);
		if (response.getStatus() < 200 || (response.getStatus() > 299 && response.getStatus()!=400)) { //supress keyspace already exists
			Map<String, String> map = response.getEntity(Map.class);
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus() + "- " + map);
		}

	}

	public void createVotingTable(){
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
		String url = musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/votecount";
		System.out.println("create url:"+url);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json")
				.type("application/json").post(ClientResponse.class, jtab);

		System.out.println(response.getEntity(Map.class));
		if (response.getStatus() < 200 || (response.getStatus() > 299 && response.getStatus()!=400)) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}
	private void checkMusicVersion(){
		Client client = Client.create();
		System.out.println(musicHandle.getMusicNodeURL()+"/version");
		

//		System.setProperty("sun.net.http.allowRestrictedHeaders", "true");
		WebResource webResource = client
				.resource(musicHandle.getMusicNodeURL()+"/version");
		

		ClientResponse response = addMusicHeaders(webResource)
				.accept(MediaType.APPLICATION_JSON).header("Connection", "close").get(ClientResponse.class);
		
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(Map.class).toString();

		System.out.println(output);

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

		String url = musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/votecount/rows";
		WebResource webResource = client
				.resource(url);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url+" candidate name:"+ candidateName);


	}

	private  String createLock(String lockName){
		Client client = Client.create();
		String msg = musicHandle.getMusicNodeURL()+"/locks/create/"+lockName;
		WebResource webResource = client.resource(msg);
		System.out.println(msg);
		WebResource.Builder wb = addMusicHeaders(webResource).accept(MediaType.APPLICATION_JSON);

		ClientResponse response = wb.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+msg);
		}

		Map<String,Object> responseMap = response.getEntity(Map.class);

		String lockid = ((Map<String,String>) responseMap.get("lock")).get("lock");
//		System.out.println("Server response .... \n");
//		System.out.println(output);
		return lockid;
	}

	private  boolean acquireLock(String lockId){
		Client client = Client.create();
		String msg = musicHandle.getMusicNodeURL()+"/locks/acquire/"+lockId;
		System.out.println(msg);
		WebResource webResource = client.resource(msg);


		WebResource.Builder wb = addMusicHeaders(webResource).accept(MediaType.APPLICATION_JSON);

		ClientResponse response = wb.get(ClientResponse.class);

		Map<String, Object> responseMap = response.getEntity(Map.class);
		
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+ ":" + responseMap);
		}

		System.out.println(responseMap);
		Boolean status = responseMap.get("status").equals("SUCCESS");
		System.out.println("Server response .... " + status);
		return status;
	}

	private  void unlock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(musicHandle.getMusicNodeURL()+"/locks/release/"+lockId);

		ClientResponse response = addMusicHeaders(webResource).delete(ClientResponse.class);


		if (response.getStatus() < 200 || response.getStatus()>299) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}

	private  void updateVoteCountAtomically(String candidateName,int count){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */
		System.out.println("trying to acquire lock!");

		String lockName = keyspaceName+".votecount."+candidateName;
		lockNames.add(lockName);
		String lockId = createLock(lockName);
		while(acquireLock(lockId) != true);
		
		System.out.println("acquired lock!");
		//update candidate entry if you have the lock
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("count",count);

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "critical");
		consistencyInfo.put("lockId", lockId);

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		System.out.println(url);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		Map<String,String> map = response.getEntity(Map.class);
		
		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+":"+map);

		//release lock now that the operation is done
		unlock(lockId);

	}

	private  void deleteCandidateEntryEventually(String candidateName){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonDelete jDel = new JsonDelete();
		jDel.setConsistencyInfo(consistencyInfo);
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		System.out.println(url);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json")
				.type("application/json").delete(ClientResponse.class, jDel);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);

	}

	public  Map<String,Object> readVoteCountForCandidate(String candidateName){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/votecount/rows?name="+candidateName;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}

	public  Map<String,Object> readAllVotes(){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		String url = musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/votecount/rows";
		WebResource webResource = client
				.resource(url);

		ClientResponse response = addMusicHeaders(webResource).accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}
	
	
	/*
	 * Unable to use this because of the error: 
	 * Exception in thread "main" com.sun.jersey.api.client.ClientHandlerException: java.net.ProtocolException: 
	 * HTTP method DELETE doesn't support output. Seems to be a error in the rest java combination according to the interwebs
	 */
	private void dropKeySpace(){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(musicHandle.getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = addMusicHeaders(webResource).type("application/json")
				.delete(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}
	
	private void deleteLock(String lockName){
		Client client = Client.create();
		WebResource webResource = client.resource(musicHandle.getMusicNodeURL()+"/locks/delete/"+lockName);

		ClientResponse response = addMusicHeaders(webResource).delete(ClientResponse.class);


		if (response.getStatus() <200 || response.getStatus()>299) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}

	private void resetMusic(){
		Client client = Client.create();
		WebResource webResource = client.resource(musicHandle.getMusicNodeURL()+"/reset");

		ClientResponse response = addMusicHeaders(webResource).delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
		
	}
	public void deleteAllLocks(){
		for (String lockName : lockNames) {
			deleteLock(lockName);
		}
	}
	
	
	public void bootStrap(){
		checkMusicVersion();
		createVotingKeyspace();


		createVotingTable();
		

		//the next few lines just create an entry in the voting table for all these candidates with vote count as 0
		createEntryForCandidate("Popeye");

		createEntryForCandidate("Judy");

		createEntryForCandidate("Flash");

		createEntryForCandidate("Mickey");

	}
	
	public void overAllTests(){
		//update the count atomically
		updateVoteCountAtomically("Popeye",5);

		updateVoteCountAtomically("Judy",7);
		
		updateVoteCountAtomically("Mickey",8);

		updateVoteCountAtomically("Flash",2);

		
		//read votecount 		
		System.out.println(readAllVotes());
		
		System.out.println(readVoteCountForCandidate("Popeye"));
		
		System.out.println(readVoteCountForCandidate("Flash"));

		deleteCandidateEntryEventually("Mickey");

		System.out.println(readAllVotes());

//		dropKeySpace();

		deleteAllLocks();
	}
	
	public void flipTest(){
		checkMusicVersion();
	}
	
	public static String executeBashScript(String pathToScript, String arg1, String arg2){
		try {
			ProcessBuilder pb = new ProcessBuilder(pathToScript,arg1, arg2);
			final Process process = pb.start();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			return br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;	
	}
	
	public static void main(String[] args) {	
		long start = System.currentTimeMillis();

		if (args.length==0) {
			args = new String[]{"localhost"};
		}
		for(int i =0; i < 2;++i){
			VotingAppREST vHandle = new VotingAppREST(args);
			vHandle.overAllTests();

			System.out.println("=====================================");
			System.out.println("Test no."+i+" completed:");
		}
		long diff = 	System.currentTimeMillis() - start;
		System.out.println(diff);
	}
			

}