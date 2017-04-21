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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import com.att.research.music.client.MusicRestClient;
import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.lockingservice.MusicLockState;
import com.att.research.music.lockingservice.MusicLockingService;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicCoreNonStatic {

	MusicLockingService  mLockHandle = null;
	MusicDataStore   mDstoreHandle = null;
	String musicIp ="";
	final static Logger logger = Logger.getLogger(MusicCoreNonStatic.class);

	public MusicCoreNonStatic(){
		musicIp = MusicUtil.defaultMusicIp;
	}

	public MusicCoreNonStatic(String musicIp){
		this.musicIp = musicIp; 
	}

	public MusicLockingService getLockingServiceHandle(){
		logger.debug("Acquiring lock store handle");
		long start = System.currentTimeMillis();
		if(mLockHandle == null){
			mLockHandle = new MusicLockingService(musicIp);
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire lock store handle:"+(end-start));
		return mLockHandle;
	}

	public MusicDataStore getDSHandle(){
		logger.debug("Acquiring data store handle");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore(musicIp);
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire data store handle:"+(end-start));
		return mDstoreHandle;
	}                                                                            

	public void warmUp(){
		getDSHandle();
		getLockingServiceHandle();

	}

	public void initializeNode() throws Exception{
		logger.info("Initializing MUSIC node");
		/*this cannot be done in a startup routing since this depends on 
		 * obtaining the node ids from others via rest
		 */
		String keyspaceName = MusicUtil.musicInternalKeySpaceName;

		String ksQuery ="CREATE KEYSPACE  IF NOT EXISTS "+ keyspaceName +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
		generalPut(ksQuery, "eventual");

		//create table to track nodeIds;
		String tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspaceName+"."+MusicUtil.nodeIdsTable+" (public_ip text PRIMARY KEY,nodeId text);"; 
		generalPut(tabQuery, "eventual");


		ArrayList<String> allNodeIps = getDSHandle().getAllNodePublicIps();
		logger.info("Initializing music internals, node ips:"+allNodeIps);
		for (String musicNodeIp : allNodeIps) {
			MusicRestClient restHandle = new MusicRestClient(musicNodeIp);
			String nodeId = restHandle.getMusicId();
			//populate the nodeId in the node Id table
			String insertQuery = "INSERT into "+keyspaceName+"."+MusicUtil.nodeIdsTable+" (public_ip,nodeId) values('"+musicNodeIp+"','"+nodeId+"')";
			generalPut(insertQuery, "eventual");

			//create table to track eventual puts
			tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspaceName+"."+MusicUtil.evPutsTable+nodeId+" (key text PRIMARY KEY, status text);"; 
			generalPut(tabQuery, "eventual");
		}
	}

	public  void createKeyspace(String keyspaceName) throws Exception {
		Map<String, Object> repl = new HashMap<String, Object>();
		repl.put("class", "SimpleStrategy");
		repl.put("replication_factor", 1);
		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setDurabilityOfWrites("true");
		jsonKp.setReplicationInfo(repl);
		createKeyspace(keyspaceName, jsonKp);
	}

	public  void createKeyspace(String keyspaceName, JsonKeySpace kspObject) throws Exception {
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
		generalPut(query, consistency);
	}

	public boolean dropKeyspace(String keyspaceName, JsonKeySpace kspObject) throws Exception {
		String query = String.format("DROP KEYSPACE %s;", keyspaceName);
		logger.debug(query);
		generalPut(query,"eventual");
		return false;
	}

	public boolean dropKeyspace(String keyspaceName) throws Exception{
		JsonKeySpace kspObject = new JsonKeySpace();
		Map<String, String> consistencyInfo = Collections.singletonMap("type", "eventual");
		kspObject.setConsistencyInfo(consistencyInfo);
		return dropKeyspace(keyspaceName, kspObject);
	}

	public void createTable(String keyspace, String tablename, JsonTable tableObj) throws Exception {
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

	public void insertIntoTable(JsonInsert insObj, String keyspace, String tablename) throws Exception{
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

	public boolean updateTable(JsonInsert insObj,String keyspace, String tablename, @Context UriInfo info) throws Exception{
		//obtain the field value pairs of the update
		Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
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


	public String createLockReference(String lockName) throws Exception{
		logger.info("Creating lock reference for lock name:"+lockName);
		String lockId = getLockingServiceHandle().createLockId("/"+lockName);
		return lockId;
	}

	private  boolean isTableOrKeySpaceLock(String key){
		String[] splitString = key.split("\\.");
		if(splitString.length > 2)
			return false;
		else
			return true;
	}

	public  MusicLockState getMusicLockState(String key) {
		try{
			String[] splitString = key.split("\\.");
			String keyspaceName = splitString[0];
			String tableName = splitString[1];
			String primaryKey = splitString[2];
			MusicLockState mls;
			String lockName = keyspaceName+"."+tableName+"."+primaryKey;
			mls = getLockingServiceHandle().getLockState(lockName);
			return mls;
		}catch (NullPointerException e) {
			logger.debug("No lock object exists as of now..");
		}
		return null;
	}

	public  boolean  acquireLock(String key, String lockId){
		/* first check if I am on top. Since ids are not reusable there is no need to check lockStatus
		 * If the status is unlocked, then the above
		call will automatically return false.*/
		Boolean result = getLockingServiceHandle().isMyTurn(lockId);	
		if(result == false){
			logger.info("In acquire lock: Not your turn, someone else has the lock");
			return false;
		}


		//this is for backward compatibility where locks could also be acquired on just
		//keyspaces or tables.
		if(isTableOrKeySpaceLock(key) == true){
			logger.info("In acquire lock: A table or keyspace lock is no longer relevant, so returning true");
			return false; 
		}

		//if you are already the lock holder no need to sync, simply return true
		//read the lock name corresponding to the key and if the status is locked or being locked, then return false
		try{
			String currentLockHolder = getMusicLockState(key).getLockHolder();
			if(lockId.equals(currentLockHolder)){
				logger.info("In acquire lock: You already have the lock!");
				return true;
			}
		}catch (NullPointerException e) {
			logger.debug("In acquire lock:No lock object exists as of now..");
		}

		//change status to "being locked". This state transition is necessary to ensure syncing before granting the lock
		String lockHolder = null;
		MusicLockState mls = new MusicLockState(MusicLockState.LockStatus.BEING_LOCKED, lockHolder);
		getLockingServiceHandle().setLockState(key, mls);
		logger.debug("In acquire lock: Set lock state to being_locked");

		mls = new MusicLockState(MusicLockState.LockStatus.LOCKED, lockHolder);
		getLockingServiceHandle().setLockState(key, mls);
		logger.debug("In acquire lock: Set lock state to locked");

		//ensure that there are no eventual puts pending and that all replicas of key
		//have the same value
		syncAllReplicas(key);
		logger.debug("In acquire lock: synced all replicas");


		//change status to locked
		lockHolder = lockId;
		mls = new MusicLockState(MusicLockState.LockStatus.LOCKED, lockHolder);
		getLockingServiceHandle().setLockState(key, mls);
		logger.info("In acquire lock: Set lock state to locked and assigned current lock ref "+ lockId+" as holder");
		return result;
	}

	private String getNodeId(String musicNodeIP){
		String query = "select * from "+MusicUtil.musicInternalKeySpaceName+"."+MusicUtil.nodeIdsTable+" where public_ip='"+musicNodeIP+"';";
		ResultSet rs = eventualGet(query);
		List<Row> rows = rs.all();
		Row row = rows.get(0);//there should be only one row
		return row.getString("nodeId");
	}

	private void syncAllReplicas(String key){
		/*sync operation 		
		wait till all the eventual puts are done at the other music nodes (with timeout)
		and wait till all values are the same at the music nodes*/
		ArrayList<String> listOfNodePublicIps = getDSHandle().getAllNodePublicIps();
		logger.debug("In sync all replicas:public Ips of nodes:"+listOfNodePublicIps);
		boolean synced = false;
		int backOffFactor = 0; 
		long backOffTime = 50;
		while(!synced){
			MusicUtil.sleep(backOffTime*backOffFactor);
			backOffFactor = backOffFactor +1;		
			MusicDigest referenceDigest = getDigest(listOfNodePublicIps.get(0),key);//could be any of the nodes
			String referenceValue = referenceDigest.getVectorTs();
			boolean mismatch = false;
			for (int i=1; i < listOfNodePublicIps.size();i++){ 
				MusicDigest mg;
				String remoteNodePublicIp="";
				try {
					remoteNodePublicIp = listOfNodePublicIps.get(i);
					mg = getDigest(remoteNodePublicIp,key);
					if(mg == null){
						logger.debug("In sync all replicas:There is no digest from "+remoteNodePublicIp+", move on");
						continue;
					}
				} catch (NoHostAvailableException e) {
					logger.debug("In sync all replicas: Node "+remoteNodePublicIp+" not responding correctly...");
					continue;//if the host is dead we do not care about his value
				}
				if(mg.getEvPutStatus().equals("inprogress")){	
					mismatch = true;
					break;
				}
				if(referenceValue.equals(mg.getVectorTs()) == false){
					mismatch = true;
					break;
				}
			}
			if(mismatch == false)
				synced = true; 		
		}
	}

	public MusicDigest getDigest(String publicIp,String key){
		long startTime = System.currentTimeMillis();
		String nodeId = getNodeId(publicIp);
		logger.debug("In getDigest:Trying to obtain message digest from node "+nodeId+" with IP:"+publicIp);
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKey = splitString[2];

		TableMetadata tableInfo = returnColumnMetadata(keyspaceName, tableName);

		String primKeyFieldName = tableInfo.getPrimaryKey().get(0).getName();

		String queryToGetVectorTS =  "SELECT vector_ts FROM "+keyspaceName+"."+tableName+ " WHERE "+primKeyFieldName+"='"+primaryKey+"';";

		ResultSet results =null;
		MusicCoreNonStatic remoteMusic = new MusicCoreNonStatic(publicIp);
		results = remoteMusic.getDSHandle().executeEventualGet(queryToGetVectorTS);

		String vectorTs=null;
		for (Row row : results) {
			vectorTs = row.getString("vector_ts");
		}
		if(vectorTs == null)
			vectorTs = "";

		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = MusicUtil.evPutsTable+MusicUtil.getMyId();
		String queryToGetEvPutStatus =  "SELECT status FROM "+metaKeyspaceName+"."+metaTableName+ " WHERE key"+"='"+key+"';";
		results = remoteMusic.getDSHandle().executeEventualGet(queryToGetEvPutStatus);
		String evPutStatus =null;
		for (Row row : results) {
			evPutStatus = row.getString("status");
		}
		if(evPutStatus == null)
			evPutStatus = "";
		MusicDigest mg = new MusicDigest(evPutStatus,vectorTs);	
		long timeTaken = System.currentTimeMillis()-startTime;
		logger.debug("In getDigest:Obtained message digest from node "+nodeId+" with IP:"+publicIp+" "+mg+" in "+timeTaken+" ms");

		return mg;
	}

	private boolean isKeyUnLocked(String lockName){
		try {
			MusicLockState mls;
			mls = getLockingServiceHandle().getLockState(lockName);
			if(mls.getLockStatus().equals(MusicLockState.LockStatus.UNLOCKED) == false){
				logger.debug("In isKeyUnLocked:The key with lock name "+lockName+" is locked");
				return false;//someone else is holding the lock to the key
			}
		} catch (NullPointerException e) {
			logger.debug("In isKeyUnLocked:No lock has been created for this, so go ahead with the eventual put..");
		}
		return true;
	}

	public boolean eventualPut(String keyspaceName, String tableName, String primaryKey, String query) throws Exception{
		//do a cassandra write one on the meta table with status as in-progress
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String evPutTrackerTable = MusicUtil.evPutsTable+MusicUtil.getMyId();
		String metaKey = "'"+keyspaceName+"."+tableName+"."+primaryKey+"'";

		String queryToUpdateEvPut =  "Insert into "+metaKeyspaceName+"."+evPutTrackerTable+"  (key,status) values ("+metaKey+",'inprogress');";   
		getDSHandle().executePut(queryToUpdateEvPut, "eventual");

		boolean result; 
		String lockName = keyspaceName+"."+tableName+"."+primaryKey;
		if(isKeyUnLocked(lockName) == false){
			result = false;
		}else{
			logger.debug("In eventual put: The key is un-locked, can perform eventual puts..");
			//do actual write 
			getDSHandle().executePut(query, "eventual");
			result = true;
		}

		//clean up meta table
		String queryToResetEvPutStatus =  "Delete from "+metaKeyspaceName+"."+evPutTrackerTable+" where key="+metaKey+";";   
		getDSHandle().executePut(queryToResetEvPutStatus, "eventual");
		return result;
	}

	public boolean criticalPut(String keyspaceName, String tableName, String primaryKey, String query, String lockId) throws Exception{
		MusicLockState mls;
		try {
			mls = getLockingServiceHandle().getLockState(keyspaceName+"."+tableName+"."+primaryKey);
		} catch (Exception e) {
			throw new Exception("NO LOCK EXISTS FOR THIS KEY");
		}
		if(mls.getLockHolder().equals(lockId)){
			String consistency = "atomic";
			getDSHandle().executePut(query,consistency);
			return true; 
		}
		else 
			throw new Exception("YOU DO NOT HAVE THE LOCK");
	}

	public  ResultSet criticalGet(String keyspaceName, String tableName, String primaryKey, String query, String lockId){
		ResultSet results = null;
		try {
			MusicLockState mls = getLockingServiceHandle().getLockState(keyspaceName+"."+tableName+"."+primaryKey);
			if(mls.getLockHolder().equals(lockId)){
				results = getDSHandle().executeCriticalGet(query);
				//				getDSHandle(MusicUtil.myCassaHost).close();
			}else
				throw new Exception("YOU DO NOT HAVE THE LOCK");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return results;
	}

	public  ResultSet eventualGet(String query){
		ResultSet results = getDSHandle().executeEventualGet(query);
		return results;
	}

	public   ResultSet quorumGet(String query){
		ResultSet results = getDSHandle().executeCriticalGet(query);
		return results;

	}

	public   Map<String, HashMap<String, Object>> marshallResults(ResultSet results){
		Map<String, HashMap<String, Object>> marshalledResults = getDSHandle().marshalData(results);
		return marshalledResults;
	}

	public   String whoseTurnIsIt(String lockName){
		String currentHolder = getLockingServiceHandle().whoseTurnIsIt("/"+lockName)+"";
		return currentHolder;
	}

	public  String getLockNameFromId(String lockId){
		StringTokenizer st = new StringTokenizer(lockId);
		String lockName = st.nextToken("$");
		return lockName;
	}

	public  void  unLock(String lockId){
		getLockingServiceHandle().unlockAndDeleteId(lockId);
		MusicLockState mls;
		String lockName = getLockNameFromId(lockId);
		String nextInLine = whoseTurnIsIt(lockName);
		if(!nextInLine.equals("")){
			mls = new MusicLockState(MusicLockState.LockStatus.LOCKED, nextInLine);
			logger.info("In unlock: lock released for "+lockId+" and given to "+nextInLine);
		}
		else{
			//change status to unlocked
			mls = new MusicLockState(MusicLockState.LockStatus.UNLOCKED, null);
			logger.info("In unlock: lock released for "+lockId+" and no one else waiting i line");
		}
		getLockingServiceHandle().setLockState(lockName, mls);
	}

	public  void deleteLock(String lockName){
		logger.info("Deleting lock for "+lockName);
		getLockingServiceHandle().deleteLock("/"+lockName);
	}

	//this is mainly for some  functions like keyspace creation etc which does not
	//really need the bells and whistles of Music locking. 
	public void generalPut(String query, String consistency) throws Exception{
		getDSHandle().executePut(query,consistency);
	}

	public TableMetadata returnColumnMetadata(String keyspace, String tablename){
		return getDSHandle().returnColumnMetadata(keyspace, tablename);
	}

	public String convertToSqlDataType(DataType type,Object valueObj){
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


	//utility function to parse json map into sql like string
	public String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter){
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
		return sqlString;	
	}

	public void pureZkCreate(String nodeName){
		getLockingServiceHandle().getzkLockHandle().createNode(nodeName);
	}

	public void pureZkWrite(String nodeName, byte[] data){
		getLockingServiceHandle().getzkLockHandle().setNodeData(nodeName, data);
	}

	public byte[] pureZkRead(String nodeName){
		return getLockingServiceHandle().getzkLockHandle().getNodeData(nodeName);
	}
}
