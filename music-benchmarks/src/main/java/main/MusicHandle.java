package main;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import javax.ws.rs.core.MediaType;

import jsonAdapters.JsonInsert;
import jsonAdapters.JsonKeySpace;
import jsonAdapters.JsonSelect;
import jsonAdapters.JsonTable;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MusicHandle {
	String[] musicNodes;
	String bmKeySpace, bmTable;
	public String rowId;
	ClientConfig clientConfig;
	Client client; 
	final int repFactor;
	public MusicHandle(String[] musicNodes, int repFactor){
		this.musicNodes = musicNodes;
		this.repFactor=repFactor;
		
		bmKeySpace = "BmKeySpace";
		bmTable = "BmEmployees";
		
		clientConfig = new DefaultClientConfig();

		clientConfig.getFeatures().put(
				JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);

		client = Client.create(clientConfig);
	}

	public void initialize(int numEntries){
		System.out.println("Begin MUSIC initialization..");

		createKeyspaceEventual(bmKeySpace);
		System.out.println("Keyspace "+bmKeySpace+" created...");

		
		//create table
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("name", "text");
		fields.put("job", "text");
		fields.put("PRIMARY KEY", "(name)");
		createTableEventual(bmKeySpace, bmTable, fields);
		System.out.println("Table "+bmTable+" created...");

		//fill rows in the table
		for(int i = 0; i < numEntries; ++i){
			Map<String,Object> values = new HashMap<String,Object>();
		    values.put("name", "emp"+i);
		    values.put("job", "researcher");
		    insertIntoTableEventual(bmKeySpace, bmTable,values);
		}
		System.out.println(numEntries + " rows inserted...");
		
		//set up zookeeper for direct operations
		
		Map<String,Object> values = new HashMap<String,Object>();
	    values.put("name", "emp"+0);
	    values.put("job", "researcher");

	    createZkNode(bmKeySpace, bmTable, values);
		
		System.out.println("Initial set up completed.");
	}
	
	private String getMusicNodeIp(){
		Random r = new Random();
		int index = r.nextInt(musicNodes.length);	
		return musicNodes[index];
	}

	private String getMusicNodeURL(){
		String musicURL = "http://"+getMusicNodeIp()+":8080/MUSIC/rest";
		return musicURL;
	}

	private void createZkNode(String keyspaceName, String tableName, Map<String,Object> values){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);


		WebResource webResource = client
				.resource(getMusicNodeURL()+"/benchmarks/purezk/bmObject");

		ClientResponse response = webResource.accept("application/json").header("Connection", "close").type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
	}


	public void createKeyspaceEventual(String keyspaceName){
		Map<String,Object> replicationInfo = new HashMap<String, Object>();
		replicationInfo.put("class", "SimpleStrategy");
		replicationInfo.put("replication_factor", repFactor);
		String durabilityOfWrites="true";
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");
		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);
		jsonKp.setDurabilityOfWrites(durabilityOfWrites);
		jsonKp.setReplicationInfo(replicationInfo);

		String queryURL =getMusicNodeURL()+"/keyspaces/"+keyspaceName; 
		WebResource webResource = client
				.resource(queryURL);
		
		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
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


		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
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
		WebResource webResource = client
				.resource(getMusicNodeURL()+"/keyspaces/"+keyspaceName+"/tables/"+tableName+"/rows");

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").post(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());


	}
	
	private  String createLock(String lockName){
		String msg = getMusicNodeURL()+"/locks/create/"+lockName;
		WebResource webResource = client.resource(msg);
		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.header("Connection", "close").post(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+msg);
		}

		String output = response.getEntity(String.class);

		return output;
	}

	private  boolean acquireLock(String lockId){
		String msg = getMusicNodeURL()+"/locks/acquire/"+lockId;
		WebResource webResource = client.resource(msg);


		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);

		ClientResponse response = wb.header("Connection", "close").get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+msg);
		}

		String output = response.getEntity(String.class);
		Boolean status = Boolean.parseBoolean(output);
		return status;
	}

	private  void unlock(String lockId){
		WebResource webResource = client.resource(getMusicNodeURL()+"/locks/release/"+lockId);

		ClientResponse response = webResource.header("Connection", "close").delete(ClientResponse.class);


		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}


	
	
	
	//the actual bm operations
	
	public void musicEvPut(){
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("job","musicEvPut"+System.currentTimeMillis());

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);

		String url = getMusicNodeURL()+"/keyspaces/"+bmKeySpace+"/tables/"+bmTable+"/rows?name="+rowId;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);	
	}


	
	public void musicCriticalPut(){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */

		String lockName = bmKeySpace+"."+bmTable+"."+rowId;
		String lockId = createLock(lockName);
		while(acquireLock(lockId) != true);
		
		//update candidate entry if you have the lock
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("job","musicCriticalPut"+System.currentTimeMillis());

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "critical");
		consistencyInfo.put("lockId", lockId);

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		String url = getMusicNodeURL()+"/keyspaces/"+bmKeySpace+"/tables/"+bmTable+"/rows?name="+rowId;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);

		//release lock now that the operation is done
		unlock(lockId);
	}
	
	public void musicAtomicPut(){
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("job","musicAtomicPut"+System.currentTimeMillis());

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		String url = getMusicNodeURL()+"/keyspaces/"+bmKeySpace+"/tables/"+bmTable+"/rows?name="+rowId;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);
	}
	
	public void musicEvGet(){
		String url = getMusicNodeURL()+"/keyspaces/"+bmKeySpace+"/tables/"+bmTable+"/rows?name="+rowId;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.header("Connection", "close").accept("application/json").get(ClientResponse.class);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus());
		
		Map<String,Object> output = response.getEntity(Map.class);
	}


	public void musicAtomicGet(){
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");

		JsonSelect jSel = new JsonSelect();
		jSel.setConsistencyInfo(consistencyInfo);
		String url = getMusicNodeURL()+"/keyspaces/"+bmKeySpace+"/tables/"+bmTable+"/rows/criticalget?name="+rowId;
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jSel);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);

	}

	public void zkNormalPut(){
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("name",rowId);
		values.put("job","zkCriticalPut"+System.currentTimeMillis());

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "critical");
		consistencyInfo.put("lockId", "no-lock");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		String url = getMusicNodeURL()+"/benchmarks/purezk/bmObject";
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);

	}

	public void zkAtomicPut(){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */

		String lockName = bmKeySpace+"."+bmTable+"."+rowId;
		String url = getMusicNodeURL()+"/benchmarks/purezk/atomic/"+lockName+"/bmObject";

		Map<String,Object> values = new HashMap<String,Object>();
		values.put("name",rowId);
		values.put("job","zkAtomicPut"+System.currentTimeMillis());

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");
		consistencyInfo.put("lockId", "no-lock");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		WebResource webResource = client
				.resource(url);

		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jIns);

		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);
	}	

	public void zkAtomicGet(){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */

		String lockName = bmKeySpace+"."+bmTable+"."+rowId;
		String url = getMusicNodeURL()+"/benchmarks/purezk/atomic/"+lockName+"/bmObject";

		Map<String,Object> values = new HashMap<String,Object>();
		values.put("name",rowId);
		values.put("job","zkAtomicPut"+System.currentTimeMillis());

		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "atomic");
		consistencyInfo.put("lockId", "no-lock");

		JsonInsert jIns = new JsonInsert();
		jIns.setValues(values);
		jIns.setConsistencyInfo(consistencyInfo);
		WebResource webResource = client
				.resource(url);
		
		
		ClientResponse response = webResource.accept("application/json").header("Connection", "close")
				.type("application/json").put(ClientResponse.class, jIns);


		if (response.getStatus() < 200 || response.getStatus() > 299) 
			throw new RuntimeException("Failed : HTTP error code : "+ response.getStatus()+"url:"+url);
	}	

	public void zkCriticalPut(){
		/*create lock for the candidate. The music API dictates that
		 * the lock name must be of the form keyspacename.tableName.primaryKeyName
		 * */

		String lockName = bmKeySpace+"."+bmTable+"."+rowId;
		String lockId = createLock(lockName);
		while(acquireLock(lockId) != true);
		
		zkNormalPut();
		//update candidate entry if you have the lock

		//release lock now that the operation is done
		unlock(lockId);
	}	
}



