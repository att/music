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
package musicinterface;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import protocol.HalUtil;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class RestMusicFunctions {


	public static void createKeyspaceEventual(String keyspaceName){
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
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	public static void createTableEventual(String keyspaceName, String tableName, Map<String,String> fields){
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
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jtab);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	public static void insertIntoTableEventual(String keyspaceName, String tableName, Map<String,Object> values){
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
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows");

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());


	}
	
	public static void updateTableEventual(String keyspaceName, String tableName, String keyName, String keyValue, Map<String,Object> values){

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
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+keyName+"="+keyValue);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
	}

	public  static Map<String,Object> readSpecificRow(String keyspaceName, String tableName,String keyName, String keyValue){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		WebResource webResource = client
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+keyName+"="+keyValue);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		
		Map<String, Object> rowMap=null;
		for (Map.Entry<String, Object> entry : output.entrySet()){
			rowMap = (Map<String, Object>)entry.getValue();
			break;
		}

		return rowMap;	
	}
	
	public  static Map<String,Object> readAllRows(String keyspaceName, String tableName){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		WebResource webResource = client
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows");

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}

	public static void dropTable(String keyspaceName, String tableName){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonTable jsonTb = new JsonTable();
		jsonTb.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName);

		ClientResponse response = webResource.type("application/json")
				.delete(ClientResponse.class, jsonTb);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}

	public static void dropKeySpace(String keyspaceName){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = webResource.type("application/json")
				.delete(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}
	
	public static String createLockRef(String lockName){
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/create/"+lockName);

		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

		return output;
	}
	
	public  static boolean acquireLock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/acquire/"+lockId);


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

	public  static String whoIsLockHolder(String lockName){
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/enquire/"+lockName);


		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
	//	System.out.println("Server response .... \n");
	//	System.out.println(output);
		return output;
	}

	public  static void unlock(String lockId){
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/release/"+lockId);

		ClientResponse response = webResource.delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}
	
	public static void main(String[] args){
		Map<String,Object> results = RestMusicFunctions.readAllRows("votingappbharath", "replicas");
		for (Map.Entry<String, Object> entry : results.entrySet()){
			Map<String, Object> valueMap = (Map<String, Object>)entry.getValue();
			for (Map.Entry<String, Object> rowentry : valueMap.entrySet()){
				if(rowentry.getKey().equals("timeoflastupdate")){
					System.out.println(rowentry.getValue());
				}
				break;
			}
			break;
		}
	}
	

}



