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
package com.att.research.music.rest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
//import java.util.logging.Level;


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

import com.att.research.music.datastore.JsonDelete;
import com.att.research.music.datastore.JsonInsert;
import com.att.research.music.datastore.JsonKeySpace;
import com.att.research.music.datastore.JsonSelect;
import com.att.research.music.datastore.JsonTable;
import com.att.research.music.datastore.JsonUpdate;
import com.att.research.music.datastore.RowIdentifier;
import com.att.research.music.main.MusicCore;
import com.att.research.music.main.MusicCore.Condition;
import com.att.research.music.main.MusicUtil;
import com.att.research.music.main.ReadReturnType;
import com.att.research.music.main.WriteReturnType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;


@Path("/")
public class RestMusicDataAPI {
	final static Logger logger = Logger.getLogger(RestMusicDataAPI.class);

	@GET
	@Path("/version")
	@Produces(MediaType.TEXT_PLAIN)
	public String version() {
		logger.info("Replying to request for MUSIC version with:"+MusicUtil.getVersion());
		return "MUSIC:"+MusicUtil.getVersion();
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

	@POST
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspaceName) throws Exception{
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
		MusicCore.nonKeyRelatedPut(query, consistency);
		
		String internalKpQuery = "CREATE KEYSPACE IF NOT EXISTS music_internal WITH replication = " + 
				repString;
		if(kspObject.getDurabilityOfWrites() != null)
			internalKpQuery = internalKpQuery +" AND durable_writes = " + kspObject.getDurabilityOfWrites() ;
		internalKpQuery = internalKpQuery + ";";

		MusicCore.nonKeyRelatedPut(internalKpQuery, consistency);
		String internalTabQuery = "CREATE TABLE IF NOT EXISTS music_internal.unsynced_keys (key text PRIMARY KEY);";
		
		MusicCore.nonKeyRelatedPut(internalTabQuery, consistency);

	}

	@DELETE
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void dropKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspaceName) throws Exception{ 
		//	String consistency = kspObject.getConsistencyInfo().get("type");
		String consistency = "eventual";//for now this needs only eventual consistency

		String query ="DROP KEYSPACE "+ keyspaceName+";"; 
		MusicCore.nonKeyRelatedPut(query, consistency);
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
		/*		
		if(tableObj.getSortingKey() != null){
			propertiesString = propertiesString + " CLUSTERING ORDER BY ("+tableObj.getSortingKey()+ " "+ 
						tableObj.getSortingOrder()+")";	
		}
		 */
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
		MusicCore.nonKeyRelatedPut(query, consistency);
	}

	
	
	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}/index/{field}")
	public void createIndex(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @PathParam("field") String fieldName,@Context UriInfo info) throws Exception{
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String indexName="";
		if(rowParams.getFirst("index_name") != null)
			indexName = rowParams.getFirst("index_name");	
		String query = "Create index "+indexName+" if not exists on "+keyspace+"."+tablename+" ("+fieldName+");";
		MusicCore.nonKeyRelatedPut(query, "eventual");
	}
	
	
	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public void insertIntoTable(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{
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
				primaryKey = primaryKey.replace("'", "''");
			}

			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToCQLDataType(colType, valueObj);
			valueString = valueString + formattedValue;
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

		//System.out.println(valueString);
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
			MusicCore.eventualPut(query);
		else if(consistency.equalsIgnoreCase("critical")){
			String lockId = insObj.getConsistencyInfo().get("lockId");
			MusicCore.criticalPut(keyspace,tablename,primaryKey, query, lockId, null);
		}
		else if(consistency.equalsIgnoreCase("atomic")){
			int batchSize;
			if(insObj.getBatchSize() != 0)
				batchSize = insObj.getBatchSize();
			else 
				batchSize =1;
			MusicCore.atomicPut(keyspace,tablename,primaryKey, query,null,batchSize);
		}
	}
	
	

	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String updateTable(JsonUpdate updateObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		String consistency = updateObj.getConsistencyInfo().get("type");
		logger.info("--------------Music "+consistency+" update-"+operationId+"-------------------------");
		//obtain the field value pairs of the update
		Map<String,Object> valuesMap =  updateObj.getValues();
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String fieldValueString="vector_ts="+vectorTs+",";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			Object valueObj = entry.getValue();	
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String valueString = MusicCore.convertToCQLDataType(colType,valueObj);	
			fieldValueString = fieldValueString+ entry.getKey()+"="+valueString;
			if(counter!=valuesMap.size()-1)
				fieldValueString = fieldValueString+",";
			counter = counter +1;
		}

		
		String ttl = updateObj.getTtl();
		String timestamp = updateObj.getTimestamp();

		String updateQuery =  "UPDATE "+keyspace+"."+tablename+" ";   
		if((ttl != null) && (timestamp != null)){
			updateQuery = updateQuery + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}

		if((ttl != null) && (timestamp == null)){
			updateQuery = updateQuery + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			updateQuery = updateQuery + " USING TIMESTAMP "+ timestamp;
		}
		
		//get the row specifier
		RowIdentifier rowId = getRowIdentifier(keyspace, tablename,  info.getQueryParameters());

		updateQuery = updateQuery + " SET "+fieldValueString+" WHERE "+rowId.rowIdString+";";
		
		//get the conditional, if any
		Condition conditionInfo;
		if(updateObj.getConditions() == null)
			conditionInfo = null;
		else{//to avoid parsing repeatedly, just send the select query to obtain row
			String selectQuery =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowId.rowIdString+";"; 
			conditionInfo = new MusicCore.Condition(updateObj.getConditions() , selectQuery);
		}


		WriteReturnType operationResult=null;
		long jsonParseCompletionTime = System.currentTimeMillis();
		try {
			if(consistency.equalsIgnoreCase("eventual"))
				operationResult = MusicCore.eventualPut(updateQuery);
			else if(consistency.equalsIgnoreCase("critical")){
				String lockId = updateObj.getConsistencyInfo().get("lockId");
				operationResult = MusicCore.criticalPut(keyspace,tablename,rowId.primarKeyValue, updateQuery, lockId, conditionInfo);
			}
			else if(consistency.equalsIgnoreCase("atomic_delete_lock")){//this function is mainly for the benchmarks
				operationResult = MusicCore.atomicPutWithDeleteLock(keyspace,tablename,rowId.primarKeyValue, updateQuery,conditionInfo);
			}
			else if(consistency.equalsIgnoreCase("atomic")){
				int batchSize;
				if(updateObj.getBatchSize() != 0)
					batchSize = updateObj.getBatchSize();
				else 
					batchSize =1;
				operationResult = MusicCore.atomicPut(keyspace,tablename,rowId.primarKeyValue, updateQuery,conditionInfo,batchSize);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		long actualUpdateCompletionTime = System.currentTimeMillis();

		long endTime = System.currentTimeMillis();
		String timingString = "Time taken in ms for Music "+consistency+" update-"+operationId+":"+"|total operation time:"+
			(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|update time:"+(actualUpdateCompletionTime-jsonParseCompletionTime)+"|";
		
		if(operationResult.getTimingInfo() != null){
			String lockManagementTime = operationResult.getTimingInfo();
			timingString = timingString+lockManagementTime;
		}
		logger.info(timingString);	
		//System.out.println(timingString);
		return operationResult.toString();
	}
	

	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public String deleteFromTable(JsonDelete delObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{ 
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
		
		//get the row specifier
		RowIdentifier rowId = getRowIdentifier(keyspace, tablename,  info.getQueryParameters());
		String rowSpec = rowId.rowIdString;
		String primaryKeyValue = rowId.primarKeyValue;
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

		
		//get the conditional, if any
		Condition conditionInfo;
		if(delObj.getConditions() == null)
			conditionInfo = null;
		else{//to avoid parsing repeatedly, just send the select query to obtain row
			String selectQuery =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowId.rowIdString+";"; 
			conditionInfo = new MusicCore.Condition(delObj.getConditions() , selectQuery);
		}


		String consistency = delObj.getConsistencyInfo().get("type");
		WriteReturnType operationResult=null;

		if(consistency.equalsIgnoreCase("eventual"))
			operationResult = MusicCore.eventualPut(query);
		else if(consistency.equalsIgnoreCase("critical")){
			String lockId = delObj.getConsistencyInfo().get("lockId");
			operationResult = MusicCore.criticalPut(keyspace,tablename,rowId.primarKeyValue, query, lockId, conditionInfo);
		}
		else if(consistency.equalsIgnoreCase("atomic")){
			int batchSize = 1; 
			operationResult = MusicCore.atomicPut(keyspace,tablename,rowId.primarKeyValue, query,conditionInfo,batchSize);
		}
		return operationResult.toString();
	}

	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	public void dropTable(JsonTable tabObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		//	String consistency = kspObject.getConsistencyInfo().get("type");
		String consistency = "eventual";//for now this needs only eventual consistency
		String query ="DROP TABLE IF EXISTS "+ keyspace+"."+tablename+";"; 
		MusicCore.nonKeyRelatedPut(query, consistency);
	}

	private RowIdentifier getRowIdentifier(String keyspace,String tablename, MultivaluedMap<String, String> rowParams){
		String rowIdString="";
		int counter =0;
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		String primaryKeyValue="";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToCQLDataType(colType,indValue);	
			if(counter ==0)
				primaryKeyValue = primaryKeyValue+indValue;
			rowIdString = rowIdString + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowIdString = rowIdString+" AND ";
			counter = counter +1;
		}
		return new RowIdentifier(primaryKeyValue, rowIdString);	
	}
	
	public String selectSpecificQuery(String keyspace,String tablename, UriInfo info, int limit){	
		String rowIdString = getRowIdentifier(keyspace, tablename, info.getQueryParameters()).rowIdString;

		String query =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowIdString; 

		if(limit != -1){
			query = query + " LIMIT "+limit;
		}

		query = query + ";";
		return query; 
	} 


	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows/criticalget")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> selectCritical(JsonSelect selObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		String consistency = selObj.getConsistencyInfo().get("type");
		logger.info("--------------Music "+consistency+" select-"+operationId+"-------------------------");

		String lockId = selObj.getConsistencyInfo().get("lockId");

		RowIdentifier rowId = getRowIdentifier(keyspace, tablename,  info.getQueryParameters());

		String selectQuery =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowId.rowIdString+";"; 

		
		long jsonParseCompletionTime = System.currentTimeMillis();

		ReadReturnType operationResult =null;
		if(consistency.equalsIgnoreCase("critical")){
			operationResult =  MusicCore.criticalGet(keyspace, tablename, rowId.primarKeyValue, selectQuery, lockId);
		}
		else if(consistency.equalsIgnoreCase("atomic")){
			operationResult = MusicCore.atomicGet(keyspace,tablename,rowId.primarKeyValue, selectQuery);
		}
		else if(consistency.equalsIgnoreCase("atomic_delete_lock")){
			operationResult = MusicCore.atomicGetWithDeleteLock(keyspace,tablename,rowId.primarKeyValue, selectQuery);
		}

		long actualSelectTime = System.currentTimeMillis();
		
		long endTime = System.currentTimeMillis();
		String timingString = "Time taken in ms for Music "+consistency+" select-"+operationId+":"+"|total operation time:"+
			(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|select time:"+(actualSelectTime-jsonParseCompletionTime)+"|";
		
		if(operationResult.getTimingInfo() != null){
			String lockManagementTime = operationResult.getTimingInfo();
			timingString = timingString+lockManagementTime;
		}
		logger.info(timingString);	


		return MusicCore.marshallResults(operationResult.getPayload());
	}


	@GET
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> select(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		logger.info("--------------Music eventual select-"+operationId+"-------------------------");

		String query ="";
		if(info.getQueryParameters().isEmpty())//select all
			query =  "SELECT *  FROM "+keyspace+"."+tablename+ ";"; 
		else{
			int limit =-1; //do not limit the number of results
			query = selectSpecificQuery(keyspace,tablename,info,limit);
		}
		long jsonParseCompletionTime = System.currentTimeMillis();

		ResultSet results = MusicCore.get(query);
		
		long actualSelectTime = System.currentTimeMillis();
		
		long endTime = System.currentTimeMillis();
		String timingString = "Time taken in ms for Music eventual select-"+operationId+":"+"|total operation time:"+
			(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|select time:"+(actualSelectTime-jsonParseCompletionTime)+"|";
		logger.info(query);
		logger.info(timingString);	
		return MusicCore.marshallResults(results);
	} 
	

	
}
