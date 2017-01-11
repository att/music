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
import java.util.StringTokenizer;

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

import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.datastore.jsonobjects.JsonDelete;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.lockingservice.MusicLockingService;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;


@Path("/")
public class RestMusic {
	MusicLockingService mLockHandle = null;
	MusicDataStore mDstoreHandle = null;
	
	public RestMusic(){
		System.out.println("created.....");
	}
	private MusicLockingService getLockingServiceHandle(){
		return MusicCore.getLockingServiceHandle();
	}

	private MusicDataStore getDSHandle(){
		return MusicCore.getDSHandle();
	}
	
	protected void finalize(){
	}

	@GET
	@Path("/version")
	@Produces(MediaType.TEXT_PLAIN)
	public String version() {
		return MusicUtil.version+" cassa host:"+MusicUtil.myCassaHost+" zk host:"+MusicUtil.myZkHost+" "+ "main "+MusicUtil.version;
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
	

/*	puts the requesting process in the q for this lock. The corresponding node will be
	created in zookeeper if it did not already exist*/
	@POST
	@Path("/locks/create/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String createLock(@PathParam("lockname") String lockName){
		String lockId = getLockingServiceHandle().createLockId("/"+lockName);
		return lockId;
	}

	//checks if the node is in the top of the queue and hence acquires the lock
	@GET
	@Path("/locks/acquire/{lockhandle}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String accquireLock(@PathParam("lockhandle") String lockId){
		String result = getLockingServiceHandle().isMyTurn(lockId)+"";
		return result;
	}

	@GET
	@Path("/locks/enquire/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String currentLockHolder(@PathParam("lockname") String lockName){
		String currentHolder = getLockingServiceHandle().whoseTurnIsIt("/"+lockName)+"";
		return currentHolder;
	}


	//deletes the process from the zk queue
	@DELETE
	@Path("/locks/release/{lockhandle}")
	public void unLock(@PathParam("lockhandle") String lockId){
		System.out.println("In the non-formal release, lock id is:"+lockId);
		getLockingServiceHandle().unlockAndDeleteId(lockId);
	}

	@DELETE
	@Path("/locks/delete/{lockname}")
	public void deleteLock(@PathParam("lockname") String lockName){
		getLockingServiceHandle().deleteLock("/"+lockName);
	}

	@POST
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspaceName) throws Exception{
		String consistency = extractConsistencyInfo(keyspaceName, kspObject.getConsistencyInfo());
		Map<String,Object> replicationInfo = kspObject.getReplicationInfo();
		String repString = "{"+jsonMaptoSqlString(replicationInfo,",")+"}";
		String query ="CREATE KEYSPACE IF NOT EXISTS "+ keyspaceName +" WITH replication = " + 
				repString;
		if(kspObject.getDurabilityOfWrites() != null)
			query = query +" AND durable_writes = " + kspObject.getDurabilityOfWrites() ;
		query = query + ";";
		System.out.println(query);
		getDSHandle().executePutQuery(query,consistency);
	}
 	

	@DELETE
	@Path("/keyspaces/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void dropKeySpace(JsonKeySpace  kspObject,@PathParam("name") String keyspaceName) throws Exception{ 
		String consistency = extractConsistencyInfo(keyspaceName, kspObject.getConsistencyInfo());
		String query ="DROP KEYSPACE "+ keyspaceName+";"; 
		System.out.println(query);
		getDSHandle().executePutQuery(query,consistency);

	}
	

	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createTable(JsonTable tableObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		String consistency = extractConsistencyInfo(keyspace+"."+tablename, tableObj.getConsistencyInfo());

		//first read the information about the table fields
		Map<String,String> fields = tableObj.getFields();
		String fieldsString="(";
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
//		System.out.println("fields:"+fieldsString);
		
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
					System.out.println("string:"+ ot);
					value = "'"+value+"'";
				}else if(ot instanceof Map){
					System.out.println("map:"+ ot);
					Map<String,Object> otMap = (Map<String,Object>)ot;
					value = "{"+jsonMaptoSqlString(otMap, ",")+"}";
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
		
		
		//along with the main table also create the table that tracks the eventual puts to each key
//		String metaTableFieldsString = "("+primaryKey+" TEXT,"+ musicNodeId+ "TEXT,"+ "PRIMARY KEY ("+primaryKey+","+musicNodeId+")"+");"; 
//		String metaTableQuery =  "CREATE TABLE IF NOT EXISTS "+keyspace+"."+tablename+"PendingEvPuts"+" "+ fieldsString; 

		System.out.println(query);
		try {
			getDSHandle().executePutQuery(query,consistency);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	@POST
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	public void insertIntoTable(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{
		String consistency = extractConsistencyInfo(keyspace+"."+tablename, insObj.getConsistencyInfo());
		Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = getDSHandle().returnColumnMetadata(keyspace, tablename);
		String fieldsString="(";
		String valueString ="(";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			fieldsString = fieldsString+""+entry.getKey();
			Object valueObj = entry.getValue();	
			System.out.println(tableInfo.getColumns());
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			valueString = valueString + convertToSqlDataType(colType,valueObj);		
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
			System.out.println("both there");
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}
		
		if((ttl != null) && (timestamp == null)){
			System.out.println("ONLY TTL there");
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			System.out.println("ONLY timestamp there");
			query = query + " USING TIMESTAMP "+ timestamp;
		}
		
		query = query +";";
		System.out.println(query);
		try {
			getDSHandle().executePutQuery(query,consistency);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@PUT
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	public void updateTable(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info	) throws Exception{
		//obtain the field value pairs of the update
		Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = getDSHandle().returnColumnMetadata(keyspace, tablename);
		String fieldValueString="";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			Object valueObj = entry.getValue();	
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String valueString = convertToSqlDataType(colType,valueObj);	
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
			String formattedValue = convertToSqlDataType(colType,indValue);	
			primaryKey = primaryKey + indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
/*			String rowSpecValueString="(";
 * 			int rowSpecValueCounter =0;
			for (String indValue : valueList) {
				String formattedValue = convertToSqlDataType(colType,indValue);	
				rowSpecValueString = rowSpecValueString + formattedValue;
				if(rowSpecValueCounter==valueList.size()-1){
					rowSpecValueString = rowSpecValueString+")";
				}
				else{ 
					rowSpecValueString = rowSpecValueString+",";
				}
				rowSpecValueCounter = rowSpecValueCounter+1;
			}
			rowSpec = rowSpec + keyName +" IN "+ rowSpecValueString+" ";
*/			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}

		String consistency = extractConsistencyInfo(keyspace+"."+tablename+"."+primaryKey, insObj.getConsistencyInfo());

		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();
		
		if((ttl != null) && (timestamp != null)){
			System.out.println("both there");
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}
		
		if((ttl != null) && (timestamp == null)){
			System.out.println("ONLY TTL there");
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			System.out.println("ONLY timestamp there");
			query = query + " USING TIMESTAMP "+ timestamp;
		}
		query = query + " SET "+fieldValueString+" WHERE "+rowSpec+";";
		System.out.println(query);
		try {
			getDSHandle().executePutQuery(query,consistency);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	public void deleteFromTable(JsonDelete delObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{ 
		
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
		TableMetadata tableInfo = getDSHandle().returnColumnMetadata(keyspace, tablename);
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String primaryKey="";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = convertToSqlDataType(colType,indValue);	
			primaryKey = primaryKey+indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}
		String query ="";

		if((columnList != null) && (!rowSpec.isEmpty())){
			System.out.println("both there");
			query =  "DELETE "+columnString+" FROM "+keyspace+"."+tablename+ " WHERE "+ rowSpec+";"; 
		}

		if((columnList == null) && (!rowSpec.isEmpty())){
			System.out.println("columns not there");
			query =  "DELETE FROM "+keyspace+"."+tablename+ " WHERE "+ rowSpec+";"; 
		}

		if((columnList != null) && (rowSpec.isEmpty())){
			query =  "DELETE "+columnString+" FROM "+keyspace+"."+tablename+ ";"; 
		}
		
		
		String consistency = extractConsistencyInfo(keyspace+"."+tablename+"."+primaryKey, delObj.getConsistencyInfo());

		System.out.println(query);
		try {
			getDSHandle().executePutQuery(query,consistency);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@DELETE
	@Path("/keyspaces/{keyspace}/tables/{tablename}")
	public void dropTable(JsonTable tabObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		String consistency = extractConsistencyInfo(keyspace+"."+tablename, tabObj.getConsistencyInfo());
		String query ="DROP TABLE IF EXISTS "+ keyspace+"."+tablename+";"; 
		System.out.println(query);
		try {
			getDSHandle().executePutQuery(query,consistency);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public ResultSet selectSpecific(String keyspace,String tablename, UriInfo info){	
		String rowSpec="";
		int counter =0;
		long start = System.currentTimeMillis();
		TableMetadata tableInfo = getDSHandle().returnColumnMetadata(keyspace, tablename);
		long end = System.currentTimeMillis();
		System.out.println("In select, time taken to get column metadata:"+(end-start));
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = convertToSqlDataType(colType,indValue);	
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}

		String query =  "SELECT *  FROM "+keyspace+"."+tablename+ " WHERE "+rowSpec+";"; 
		System.out.println(query);
		ResultSet results = 	getDSHandle().executeGetQuery(query);
		return results;
	} 

	private ResultSet selectAll(String keyspace, String tablename){
		long start = System.currentTimeMillis();
		String query =  "SELECT *  FROM "+keyspace+"."+tablename+ ";"; 
		System.out.println(query);
		ResultSet results = 	getDSHandle().executeGetQuery(query);
		long end = System.currentTimeMillis();
		System.out.println("Time taken for the select all:"+(end-start));
		return results;
	}

/*	@GET
	@Path("/digest/{key}")
	@Produces(MediaType.APPLICATION_JSON)	
	public MusicDigest select(@PathParam("key") String key){
		long start = System.currentTimeMillis();
		MusicDigest mg = MusicCore.getLocalDigest(key);
		long end = System.currentTimeMillis();
		System.out.println("In the new digest function, time taken:"+(end-start));
		return mg;
	} 
*/
	@GET
	@Path("/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> select(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		long start = System.currentTimeMillis();
		ResultSet results;
		if(info.getQueryParameters().isEmpty())//select all
			results = selectAll(keyspace, tablename);
		else
			results = selectSpecific(keyspace,tablename,info);

		Map<String, HashMap<String, Object>> resultMap = new HashMap<String, HashMap<String,Object>>();
		int counter =0;
		for (Row row : results) {
			ColumnDefinitions colInfo = row.getColumnDefinitions();
			HashMap<String,Object> resultOutput = new HashMap<String, Object>();
			for (Definition definition : colInfo) {
			//	System.out.println("column name:"+ definition.getName());
				resultOutput.put(definition.getName(), getDSHandle().readRow(row, definition.getName(), definition.getType()));
			}
			resultMap.put("row "+counter, resultOutput);
			counter++;
		}
		long end = System.currentTimeMillis();
		System.out.println("time taken for select query in old rest:"+ (end-start));
		return resultMap;
	} 
	

	@POST
	@Path("/executeGetQuery")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)	
	public ArrayList<String> executeGetQuery(ArrayList<String> queryRetrievalObject){
		System.out.println(queryRetrievalObject);
		String query = queryRetrievalObject.get(0);
		ResultSet results = getDSHandle().executeGetQuery(query);
		ArrayList<String> massagedResults = new ArrayList<String>();
		for (Row row : results) {
			String rowString="|";
			for(int colNameIndex =1; colNameIndex < queryRetrievalObject.size();colNameIndex++){
				String colName = queryRetrievalObject.get(colNameIndex);
				rowString = rowString + row.getString(colName)+"|";
			}
			massagedResults.add(rowString);
		}
		return massagedResults;
	}


/*	@POST
	@Path("/executePutQuery")
	@Consumes(MediaType.APPLICATION_JSON)
	public void executePutQuery(String query){ 
		System.out.println(query);
		try {
			getDSHandle().executeCreateQuery(query,"eventual");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

*/


	@DELETE
	@Path("/executeTestQuery")
	@Consumes(MediaType.APPLICATION_JSON)
	public void executeTestQuery(HashMap<String, String> query){ 
		System.out.println(query);
	}

	//utility function to parse json map into sql like string
	private String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter){
		String sqlString="";
		int counter =0;
		for (Map.Entry<String, Object> entry : jMap.entrySet())
		{
			Object ot = entry.getValue();
			String value = ot+"";
			if(ot instanceof String){
				value = "'"+value+"'";
			}
			sqlString = sqlString+"'"+entry.getKey()+"':"+ value+"";
			if(counter!=jMap.size()-1)
				sqlString = sqlString+lineDelimiter;
			counter = counter +1;
		}	
		//System.out.println(sqlString);
		return sqlString;	
	}
	
	private String convertToSqlDataType(DataType type,Object valueObj){
		String value ="";
		switch (type.getName()) {
		case UUID:
			value = valueObj+"";
			break;
		case TEXT:
			value = "'"+valueObj+"'";
			break;
		case MAP:{
			Map<String,Object> otMap = (Map<String,Object>)valueObj;
			value = "{"+jsonMaptoSqlString(otMap, ",")+"}";
			break;
		}
		default:
			value = valueObj+"";
			break;
		}
		return value;
	}
	
	private String extractConsistencyInfo(String key, Map<String, String> consistencyInfo) throws Exception{
		String consistency="";
		if(consistencyInfo.get("type").equalsIgnoreCase("atomic")){
			String lockId = consistencyInfo.get("lockId");
			
			String lockName = lockId.substring(lockId.indexOf("$") + 1);
			lockName = lockName.substring(0, lockName.indexOf("$"));
			
			//first ensure that the lock name is correct before seeing if it has access
			if(!lockName.equalsIgnoreCase(key))
				throw new Exception("THIS LOCK IS NOT FOR THE KEY: "+ key);

			String lockStatus =  getLockingServiceHandle().isMyTurn(lockId)+"";
			if(lockStatus.equalsIgnoreCase("false"))
					throw new Exception("YOU DO NOT HAVE THE LOCK");
			consistency = "atomic";
		}else if(consistencyInfo.get("type").equalsIgnoreCase("eventual"))
			consistency = "eventual";
		else
			throw new Exception("Consistency type "+consistency+ " unknown!!");

		return consistency;
	}
	
/*	private String parseValuesBasedOnColumnType(String keyspace, String table, Map<String, Object> valuesMap){
		TableMetadata tableInfo = getDSHandle().returnColumnMetadata(keyspace, table);
		String fieldsString="(";
		String valueString ="(";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			fieldsString = fieldsString+""+entry.getKey();
			Object valueObj = entry.getValue();	
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			valueString = valueString + convertToSqlDataType(colType,valueObj);		
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
		System.out.println(fieldsString);
		System.out.println(valueString);
		return "";
	}
*/	
}

