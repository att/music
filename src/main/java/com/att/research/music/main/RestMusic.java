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
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.jsonobjects.JsonDelete;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.lockingservice.MusicLockState;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;


@Path("/")
public class RestMusic {
	final static Logger logger = Logger.getLogger(RestMusic.class);

	@GET
	@Path("/version")
	@Produces(MediaType.TEXT_PLAIN)
	public String version() {
		logger.info("Replying to request for MUSIC version with:"+MusicUtil.version);
		return "MUSIC:"+MusicUtil.version;
	}

	@GET
	@Path("/test")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, HashMap<String, String>> simpleTests() {
		Map<String, HashMap<String, String>> testMap = new HashMap<String, HashMap<String,String>>();
		for(int i=0; i < 3; i++){
			HashMap<String, String> innerMap = new HashMap<String, String>();
			innerMap.put(i+"", i+1+"");
			innerMap.put(i+1+"", i+2+"");
			testMap.put(i+"", innerMap);
		}
		return testMap;
	}
	
	@GET
	@Path("/warmup")
	public void warmup() {
		MusicCore.getDSHandle();
		MusicCore.getLockingServiceHandle();
	}

	@GET
	@Path("/warmupermote/{ip}")
	public void warmupRemote(@PathParam("ip") String remoteIp) {
		MusicCore.getDSHandle(remoteIp);
	}
	@GET
	@Path("/nodeId")
	@Produces(MediaType.TEXT_PLAIN)
	public String getMyId() {
		String nodeId = MusicUtil.getMyId();
		logger.info("Replying to request for node id with:"+nodeId);
		return nodeId;
	}


/*	puts the requesting process in the q for this lock. The corresponding node will be
	created in zookeeper if it did not already exist*/
	@POST
	@Path("/locks/create/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String createLockReference(@PathParam("lockname") String lockName){
		return MusicCore.createLockReference(lockName);
	}

	//checks if the node is in the top of the queue and hence acquires the lock
	@GET
	@Path("/locks/acquire/{lockreference}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String accquireLock(@PathParam("lockreference") String lockId){
		String lockName = lockId.substring(lockId.indexOf("$")+1, lockId.lastIndexOf("$"));
		String result = MusicCore.acquireLock(lockName,lockId)+"";
		return result; 
	}

	@GET
	@Path("/locks/enquire/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String currentLockHolder(@PathParam("lockname") String lockName){
		return MusicCore.whoseTurnIsIt(lockName);
	}

	@GET
	@Path("/locks/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String currentLockState(@PathParam("lockname") String lockName){
		MusicLockState mls = MusicCore.getMusicLockState(lockName);
		if(mls == null)
			return "No lock object created yet..";
		return mls.getLockStatus()+"|"+mls.getLockHolder();
	}

	//deletes the process from the zk queue
	@DELETE
	@Path("/locks/release/{lockreference}")
	public void unLock(@PathParam("lockreference") String lockId){
		MusicCore.unLock(lockId);
	}

	@DELETE
	@Path("/locks/delete/{lockname}")
	public void deleteLock(@PathParam("lockname") String lockName){
		MusicCore.deleteLock(lockName);
	}

	@POST
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspaceName) throws Exception{
		//first create music internal stuff by calling the initialization routine
		MusicCore.initializeNode();
		
		String consistency = "eventual";//for now this needs only eventual consistency
		long start = System.currentTimeMillis();
		Map<String,Object> replicationInfo = kspObject.getReplicationInfo();
		String repString = "{"+MusicCore.jsonMaptoSqlString(replicationInfo,",")+"}";
		String query ="CREATE KEYSPACE IF NOT EXISTS "+ keyspaceName +" WITH replication = " + 
				repString;
		if(kspObject.getDurabilityOfWrites() != null)
			query = query +" AND durable_writes = " + kspObject.getDurabilityOfWrites() ;
		query = query + ";";
		long end = System.currentTimeMillis();
		logger.debug("Time taken for setting up query in create keyspace:"+ (end-start));
		MusicCore.generalPut(query, consistency);
	}

	@DELETE
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void dropKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspaceName) throws Exception{ 
		//	String consistency = kspObject.getConsistencyInfo().get("type");
		String consistency = "eventual";//for now this needs only eventual consistency
		
		String query ="DROP KEYSPACE "+ keyspaceName+";"; 
		MusicCore.generalPut(query, consistency);
	}
	

	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createTable(JsonTable tableObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		//	String consistency = kspObject.getConsistencyInfo().get("type");
		String consistency = "eventual";//for now this needs only eventual consistency

		//first read the information about the table fields
		Map<String,String> fields = tableObj.getFields();
		String fieldsString="(vector_ts text,";
		int counter =0;
		String primaryKey;
		for (Map.Entry<String, String> entry : fields.entrySet())
		{
			fieldsString = fieldsString+""+entry.getKey()+" "+ entry.getValue()+"";
			if(entry.getKey().equals("PRIMARY KEY")){
				primaryKey = entry.getValue().substring(entry.getValue().indexOf("(") + 1);
				primaryKey = primaryKey.substring(0, primaryKey.indexOf(")"));
			}
			if(counter==fields.size()-1)
				fieldsString = fieldsString+")";
			else 
				fieldsString = fieldsString+",";
			counter = counter +1;
		}	
		
		
		//information about the name-value style properties 
		Map<String,Object> propertiesMap = tableObj.getProperties();
		String propertiesString="";
		if(propertiesMap != null){
			counter =0;
			for (Map.Entry<String, Object> entry : propertiesMap.entrySet())
			{
				Object ot = entry.getValue();
				String value = ot+"";
				if(ot instanceof String){
					value = "'"+value+"'";
				}else if(ot instanceof Map){
					Map<String,Object> otMap = (Map<String,Object>)ot;
					value = "{"+MusicCore.jsonMaptoSqlString(otMap, ",")+"}";
				}
				propertiesString = propertiesString+entry.getKey()+"="+ value+"";
				if(counter!=propertiesMap.size()-1)
					propertiesString = propertiesString+" AND ";
				counter = counter +1;
			}	
		}

		String query =  "CREATE TABLE IF NOT EXISTS "+keyspace+"."+tablename+" "+ fieldsString; 
		
		if(propertiesMap != null)
			query = query + " WITH "+ propertiesString;
		
		query = query +";";
		MusicCore.generalPut(query, consistency);
	}

	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public void insertIntoTable(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{
				Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();
		String fieldsString="(vector_ts,";
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String valueString ="("+vectorTs+",";
		int counter =0;
		String primaryKey="";
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			fieldsString = fieldsString+""+entry.getKey();
			Object valueObj = entry.getValue();	
			if(primaryKeyName.equals(entry.getKey())){
				primaryKey= entry.getValue()+"";
			}
				
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			valueString = valueString + MusicCore.convertToSqlDataType(colType,valueObj);		
			if(counter==valuesMap.size()-1){
				fieldsString = fieldsString+")";
				valueString = valueString+")";
			}
			else{ 
				fieldsString = fieldsString+",";
				valueString = valueString+",";
			}
			counter = counter +1;
		}
		
		String query =  "INSERT INTO "+keyspace+"."+tablename+" "+ fieldsString+" VALUES "+ valueString;   
		
		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();
		
		if((ttl != null) && (timestamp != null)){
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}
		
		if((ttl != null) && (timestamp == null)){
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			query = query + " USING TIMESTAMP "+ timestamp;
		}
		
		query = query +";";

		String consistency = insObj.getConsistencyInfo().get("type");
		if(consistency.equalsIgnoreCase("eventual"))
			MusicCore.eventualPut(keyspace,tablename,primaryKey, query);
		else if(consistency.equalsIgnoreCase("atomic")){
			String lockId = insObj.getConsistencyInfo().get("lockId");
			MusicCore.criticalPut(keyspace,tablename,primaryKey, query, lockId);
		}
	}

	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public boolean updateTable(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info	) throws Exception{
		//obtain the field value pairs of the update
		long start = System.currentTimeMillis();
		Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		long end = System.currentTimeMillis();
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String fieldValueString="vector_ts="+vectorTs+",";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			Object valueObj = entry.getValue();	
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String valueString = MusicCore.convertToSqlDataType(colType,valueObj);	
			fieldValueString = fieldValueString+ entry.getKey()+"="+valueString;
			if(counter!=valuesMap.size()-1)
				fieldValueString = fieldValueString+",";
			counter = counter +1;
		}
		
		//get the row specifier
		String rowSpec="";
		counter =0;
		String query =  "UPDATE "+keyspace+"."+tablename+" ";   
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String primaryKey = "";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToSqlDataType(colType,indValue);	
			primaryKey = primaryKey + indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}


		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();
		
		if((ttl != null) && (timestamp != null)){
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}
		
		if((ttl != null) && (timestamp == null)){
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			query = query + " USING TIMESTAMP "+ timestamp;
		}
		query = query + " SET "+fieldValueString+" WHERE "+rowSpec+";";
		
		String consistency = insObj.getConsistencyInfo().get("type");

		boolean operationResult = false;
		if(consistency.equalsIgnoreCase("eventual"))
			operationResult = MusicCore.eventualPut(keyspace,tablename,primaryKey, query);
		else if(consistency.equalsIgnoreCase("atomic")){
			String lockId = insObj.getConsistencyInfo().get("lockId");
			operationResult = MusicCore.criticalPut(keyspace,tablename,primaryKey, query, lockId);
		}
		return operationResult; 	
	}
	
	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public boolean deleteFromTable(JsonDelete delObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{ 
		String columnString="";
		int counter =0;
		ArrayList<String> columnList = delObj.getColumns();
		if(columnList != null){
			for (String column : columnList) {
				columnString = columnString + column;
				if(counter!=columnList.size()-1)
					columnString = columnString+",";
				counter = counter+1;
			}
		}
		String rowSpec="";
		counter =0;
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String primaryKey="";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToSqlDataType(colType,indValue);	
			primaryKey = primaryKey+indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}
		String query ="";

		if((columnList != null) && (!rowSpec.isEmpty())){
			query =  "DELETE "+columnString+" FROM "+keyspace+"."+tablename+ " WHERE "+ rowSpec+";"; 
		}

		if((columnList == null) && (!rowSpec.isEmpty())){
			query =  "DELETE FROM "+keyspace+"."+tablename+ " WHERE "+ rowSpec+";"; 
		}

		if((columnList != null) && (rowSpec.isEmpty())){
			query =  "DELETE "+columnString+" FROM "+keyspace+"."+tablename+ ";"; 
		}

		boolean operationResult = false;

		String consistency = delObj.getConsistencyInfo().get("type");
		if(consistency.equalsIgnoreCase("eventual"))
			operationResult = MusicCore.eventualPut(keyspace,tablename,primaryKey, query);
		else if(consistency.equalsIgnoreCase("atomic")){
			String lockId = delObj.getConsistencyInfo().get("lockId");
			operationResult = MusicCore.criticalPut(keyspace,tablename,primaryKey, query, lockId);
		}
		return operationResult; 
	}

	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	public void dropTable(JsonTable tabObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		//	String consistency = kspObject.getConsistencyInfo().get("type");
		String consistency = "eventual";//for now this needs only eventual consistency
		String query ="DROP TABLE IF EXISTS "+ keyspace+"."+tablename+";"; 
		MusicCore.generalPut(query, consistency);
	}

	public Map<String, HashMap<String, Object>> selectSpecific(String keyspace,String tablename, UriInfo info){	
		String rowSpec="";
		int counter =0;
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String primaryKey="";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToSqlDataType(colType,indValue);	
			primaryKey = primaryKey+indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}
		String query =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowSpec+";"; 
		ResultSet results = MusicCore.get(query);
		return MusicCore.marshallResults(results);
	} 

	private Map<String, HashMap<String, Object>> selectAll(String keyspace, String tablename){
		long start = System.currentTimeMillis();
		String query =  "SELECT *  FROM "+keyspace+"."+tablename+ ";"; 
		ResultSet results = MusicCore.get(query);
		long end = System.currentTimeMillis();
		logger.debug("Time taken for the select all:"+(end-start));
		return MusicCore.marshallResults(results);
	}

	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows/criticalget")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> criticalGet(JsonInsert selObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
			String lockId = selObj.getConsistencyInfo().get("lockId");
			
			String rowSpec="";
			int counter =0;
			TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
			MultivaluedMap<String, String> rowParams = info.getQueryParameters();
			String primaryKey="";
			for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
				String keyName = entry.getKey();
				List<String> valueList = entry.getValue();
				String indValue = valueList.get(0);
				DataType colType = tableInfo.getColumn(entry.getKey()).getType();
				String formattedValue = MusicCore.convertToSqlDataType(colType,indValue);	
				primaryKey = primaryKey+indValue;
				rowSpec = rowSpec + keyName +"="+ formattedValue;
				if(counter!=rowParams.size()-1)
					rowSpec = rowSpec+" AND ";
				counter = counter +1;
			}
			String query =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowSpec+";"; 

			ResultSet results = MusicCore.criticalGet(keyspace, tablename, primaryKey, query, lockId);
			return MusicCore.marshallResults(results);
	}
	

	@GET
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> select(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		if(info.getQueryParameters().isEmpty())//select all
			return selectAll(keyspace, tablename);
		else
			return selectSpecific(keyspace,tablename,info);
	} 

	//pure zk calls...
	@POST
	@Path("/purezk/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkCreate(@PathParam("name") String nodeName) throws Exception{
		MusicCore.pureZkCreate("/"+nodeName);
	}

	@PUT
	@Path("/purezk/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkUpdate(JsonInsert insObj,@PathParam("name") String nodeName) throws Exception{
		MusicCore.pureZkWrite(nodeName, insObj.serialize());
	}
	
	@GET
	@Path("/purezk/{name}")
	@Consumes(MediaType.TEXT_PLAIN)
	public byte[] pureZkGet(@PathParam("name") String nodeName) throws Exception{
		return MusicCore.pureZkRead(nodeName);
	}

	//pure cassa calls...
	@PUT
	@Path("purecassa/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureCassaUpdateTable(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info	) throws Exception{
		//obtain the field value pairs of the update
		long start = System.currentTimeMillis();
		Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		long end = System.currentTimeMillis();
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String fieldValueString="";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			Object valueObj = entry.getValue();	
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String valueString = MusicCore.convertToSqlDataType(colType,valueObj);	
			fieldValueString = fieldValueString+ entry.getKey()+"="+valueString;
			if(counter!=valuesMap.size()-1)
				fieldValueString = fieldValueString+",";
			counter = counter +1;
		}
		
		//get the row specifier
		String rowSpec="";
		counter =0;
		String query =  "UPDATE "+keyspace+"."+tablename+" ";   
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String primaryKey = "";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToSqlDataType(colType,indValue);	
			primaryKey = primaryKey + indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}


		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();
		
		if((ttl != null) && (timestamp != null)){
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}
		
		if((ttl != null) && (timestamp == null)){
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			query = query + " USING TIMESTAMP "+ timestamp;
		}
		query = query + " SET "+fieldValueString+" WHERE "+rowSpec+";";
		
		String consistency = insObj.getConsistencyInfo().get("type");
		MusicCore.generalPut(query, consistency);
	}
	
	@PUT
	@Path("purecassa/keyspaces/{keyspace}/tables/{tablename}/rows/quorumget")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> pureCassaCriticalGet(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){		
			String rowSpec="";
			int counter =0;
			TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
			MultivaluedMap<String, String> rowParams = info.getQueryParameters();
			String primaryKey="";
			for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
				String keyName = entry.getKey();
				List<String> valueList = entry.getValue();
				String indValue = valueList.get(0);
				DataType colType = tableInfo.getColumn(entry.getKey()).getType();
				String formattedValue = MusicCore.convertToSqlDataType(colType,indValue);	
				primaryKey = primaryKey+indValue;
				rowSpec = rowSpec + keyName +"="+ formattedValue;
				if(counter!=rowParams.size()-1)
					rowSpec = rowSpec+" AND ";
				counter = counter +1;
			}
			String query =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowSpec+";"; 

			ResultSet results = MusicCore.quorumGet(query);
			return MusicCore.marshallResults(results);
	}

}
