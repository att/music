package main;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import jsonAdapters.JsonInsert;
import jsonAdapters.JsonKeySpace;
import jsonAdapters.JsonTable;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MusicHandle {
	String[] musicNodes;
	public MusicHandle(String[] musicNodes){
		this.musicNodes = musicNodes;
	}

	private String getMusicNodeIp(){
		Random r = new Random();
		int index = r.nextInt(musicNodes.length);	
		return musicNodes[index];
	}

	private String getMusicNodeURL(){
		return "http://"+getMusicNodeIp()+":8080/MUSIC/rest/";
	}

	public void createKeyspaceEventual(String keyspaceName){
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
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jsonKp);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	public void createTableEventual(String keyspaceName, String tableName, Map<String,String> fields){
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
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName);

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jtab);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());

	}

	public void insertIntoTableEventual(String keyspaceName, String tableName, Map<String,Object> values){
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
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows");

		ClientResponse response = webResource.accept("application/json")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());


	}
	
	private  Map<String,Object> readSpecificRow(String keyspaceName, String tableName,String keyName, String keyValue){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows?"+keyName+"="+keyValue);

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}
	
	private  Map<String,Object> readAllRows(String keyspaceName, String tableName){
		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);
		
		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows");

		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
		return output;	
	}

	private void dropTable(String keyspaceName, String tableName){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonTable jsonTb = new JsonTable();
		jsonTb.setConsistencyInfo(consistencyInfo);

		ClientConfig clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		Client client = Client.create(clientConfig);

		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName);

		ClientResponse response = webResource.type("application/json")
				.delete(ClientResponse.class, jsonTb);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}

	private void dropKeySpace(String keyspaceName){
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
	
	public static void main(String[] musicIps){
		MusicHandle tc = new MusicHandle(musicIps);
		String evKeyspace = "evKeySpace"+System.currentTimeMillis();
		System.out.println("Keyspace "+evKeyspace+" created...");
		tc.createKeyspaceEventual(evKeyspace);
		String evTable = "Employees";
		
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("id", "uuid");
		fields.put("name", "text");
		fields.put("count", "varint");
		fields.put("address", "Map<text,text>");
		fields.put("PRIMARY KEY", "(name)");

		tc.createTableEventual(evKeyspace, evTable, fields);
		
		Map<String,Object> values = new HashMap<String,Object>();
	    values.put("id", UUID.randomUUID());
	    values.put("name", "bharath");
	    values.put("count", 4);
	    Map<String, String> address = new HashMap<String, String>();
	    address.put("number", "1");
	    address.put("street", "att way");
	    values.put("address", address);

	   	tc.insertIntoTableEventual(evKeyspace, evTable,values);
	    values.put("id", UUID.randomUUID());
	    values.put("name", "joe");
	    values.put("count", 10);
	    address.put("number", "5");
	    address.put("street", "thomas street");
	    values.put("address", address);
	   	
	   	tc.insertIntoTableEventual(evKeyspace, evTable,values);

	   	System.out.println(tc.readSpecificRow(evKeyspace, evTable,"name","bharath"));
	   	System.out.println(tc.readSpecificRow(evKeyspace, evTable,"name","joe"));

	   	System.out.println(tc.readAllRows(evKeyspace, evTable));
	   	
		tc.dropTable(evKeyspace, evTable);

		tc.dropKeySpace(evKeyspace);
		System.out.println("All tests passed");
	}

}



