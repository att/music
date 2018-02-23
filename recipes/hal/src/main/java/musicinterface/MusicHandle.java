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

import com.att.eelf.logging.EELFLoggerDelegate;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MusicHandle {
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicHandle.class);

	public static void createKeyspaceEventual(String keyspaceName){
		logger.info(EELFLoggerDelegate.applicationLogger, "createKeyspaceEventual"+keyspaceName);
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

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failed to createKeySpaceEventual : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}

	}

	public static void createTableEventual(String keyspaceName, String tableName, Map<String,String> fields){
		logger.info(EELFLoggerDelegate.applicationLogger,
				"createKeyspaceEventual "+keyspaceName+" tableName "+tableName);
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

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to createKeySpaceEventual : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}

	}

	public static void createIndexInTable(String keyspaceName, String tableName, String colName) {
		logger.info(EELFLoggerDelegate.applicationLogger,
				"createIndexInTable "+keyspaceName+" tableName "+tableName + " colName" + colName);
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()
				+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/index/"+colName);

		ClientResponse response = webResource.accept("application/json").post(ClientResponse.class);

		if (response.getStatus() != 200 && response.getStatus() != 204) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to createIndexInTable : Status Code " + response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}

	}

	public static void insertIntoTableEventual(String keyspaceName, String tableName, Map<String,Object> values){
		logger.info(EELFLoggerDelegate.applicationLogger,
				"insertIntoTableEventual "+keyspaceName+" tableName "+tableName);
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

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to insertIntoTableEventual : Status Code " + response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}


	}

	public static void updateTableEventual(String keyspaceName, String tableName, String keyName, String keyValue, Map<String,Object> values){
		logger.info(EELFLoggerDelegate.applicationLogger, "updateTableEventual "+keyspaceName+" tableName "+tableName);
		
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

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to updateTableEventual : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}

	}

	public  static Map<String,Object> readSpecificRow(String keyspaceName, String tableName,String keyName, String keyValue){
		logger.info(EELFLoggerDelegate.applicationLogger,
				"readSpecificRow "+keyspaceName+" tableName "+tableName + " key" +keyName + " value" + keyValue);
		
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+keyName+"="+keyValue);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to insertIntoTableEventual : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}

		Map<String,Object> output = response.getEntity(Map.class);

		Map<String, Object> rowMap=null;
		for (Map.Entry<String, Object> entry : output.entrySet()){
			rowMap = (Map<String, Object>)entry.getValue();
			break;
		}

		return rowMap;	
	}

	public  static Map<String,Object> readAllRows(String keyspaceName, String tableName){
		logger.info(EELFLoggerDelegate.applicationLogger, "readAllRows "+keyspaceName+" tableName "+tableName);
		
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(HalUtil.getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows");

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failed to readAllRows : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}

		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}

	public static void dropTable(String keyspaceName, String tableName){
		logger.info(EELFLoggerDelegate.applicationLogger, "dropTable "+keyspaceName+" tableName "+tableName);
		
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

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failed to dropTable : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}
	}

	public static void dropKeySpace(String keyspaceName){
		logger.info(EELFLoggerDelegate.applicationLogger, "dropKeySpace "+keyspaceName);
		
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

		if (response.getStatus() < 200 || response.getStatus() > 299) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failed to dropTable : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		}
	}

	public static String createLockRef(String lockName){
		logger.info(EELFLoggerDelegate.applicationLogger, "createLockRef "+lockName);
		
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/create/"+lockName);

		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.post(ClientResponse.class);

		if (response.getStatus() != 200) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to createLockRef : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}

		String output = response.getEntity(String.class);

		return output;
	}

	public  static Map<String,Object> acquireLock(String lockId){
		logger.info(EELFLoggerDelegate.applicationLogger, "acquireLock "+lockId);
		
		//should be fixed in MUSIC, but putting patch here too
		if (lockId==null) {
			Map<String,Object> fail = new HashMap<String, Object>();
			fail.put("status", "FAILURE");
			return fail;
		}

		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/acquire/"+lockId);

		ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

		if (response.getStatus() != 200) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failed to acquireLock : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}

		Map<String,Object> output = response.getEntity(Map.class);

		return output;

	}

	public static String whoIsLockHolder(String lockName){
		logger.info(EELFLoggerDelegate.applicationLogger, "whoIsLockHolder "+lockName);
		
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/enquire/"+lockName);


		WebResource.Builder wb = webResource.accept(MediaType.APPLICATION_JSON);

		ClientResponse response = wb.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			logger.error(EELFLoggerDelegate.errorLogger,
					"Failed to determine whoIsLockHolder : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}

		Map<String,String> lockoutput = (Map<String, String>) response.getEntity(Map.class).get("lock");
		if (lockoutput.get("lock-holder").equals("No lock holder!")) {
			logger.info(EELFLoggerDelegate.applicationLogger, "No lock holder");
			return null;
		}
		return (String) lockoutput.get("lock-holder");
	}

	public  static void unlock(String lockId){
		logger.info(EELFLoggerDelegate.applicationLogger, "unlock "+lockId);
		Client client = Client.create();
		WebResource webResource = client.resource(HalUtil.getMusicNodeURL()+"/locks/release/"+lockId);

		ClientResponse response = webResource.delete(ClientResponse.class);

		if (response.getStatus() != 204) {
			logger.error(EELFLoggerDelegate.errorLogger, "Failed to unlock : Status Code "+response.getStatus());
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}
	}

	public static void main(String[] args){
		Map<String,Object> results = MusicHandle.readAllRows("votingappbharath", "replicas");
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



