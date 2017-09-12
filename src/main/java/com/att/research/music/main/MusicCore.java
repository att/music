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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.att.research.music.client.MusicRestClient;
import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.lockingservice.MusicLockState;
import com.att.research.music.lockingservice.MusicLockingService;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicCore {

	static  MusicLockingService  mLockHandle = null;
	static  MusicDataStore   mDstoreHandle = null;
	final static Logger logger = Logger.getLogger(MusicCore.class);

	public static MusicLockingService getLockingServiceHandle(){
		logger.debug("Acquiring lock store handle");
		long start = System.currentTimeMillis();
		if(mLockHandle == null){
			mLockHandle = new MusicLockingService();
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire lock store handle:"+(end-start));
		return mLockHandle;
	}

	public static MusicDataStore getDSHandle(String remoteIp){
		logger.debug("Acquiring data store handle");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore(remoteIp);
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire data store handle:"+(end-start));
		return mDstoreHandle;
	}                                                                            

	public static MusicDataStore getDSHandle(){
		logger.debug("Acquiring data store handle");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore();
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire data store handle:"+(end-start));
		return mDstoreHandle;
	}        
	
	public static void initializeNode() throws Exception{
		logger.info("Initializing MUSIC node...");
		/*this cannot be done in a startup routing since this depends on 
		 * obtaining the node ids from others via rest
		 */
		String keyspaceName = MusicUtil.musicInternalKeySpaceName;
		
		String ksQuery ="CREATE KEYSPACE  IF NOT EXISTS "+ keyspaceName +" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };";
		generalPut(ksQuery, "eventual");

		PropertiesReader prop = new PropertiesReader();
		
		String[] allNodeIds = prop.getAllIds();
		for(int i=0; i < allNodeIds.length;++i){
			//create table to track eventual puts
			String tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspaceName+"."+MusicUtil.evPutsTable+allNodeIds[i]+" (key text PRIMARY KEY, status text);"; 
			generalPut(tabQuery, "eventual");
		}
	}


	public static  String createLockReference(String lockName){
		logger.info("Creating lock reference for lock name:"+lockName);
		String lockId = getLockingServiceHandle().createLockId("/"+lockName);
		return lockId;
	}

	public static boolean isTableOrKeySpaceLock(String key){
		String[] splitString = key.split("\\.");
		if(splitString.length > 2)
			return false;
		else
			return true;
	}
	
	public static MusicLockState getMusicLockState(String key){
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

	public static  boolean  acquireLock(String key, String lockId){
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
			logger.info("In acquire lock: A table or keyspace lock so no need to perform sync...so returning true");
			return true; 
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

/*	private static String getNodeId(String musicNodeIP){
		String query = "select * from "+MusicUtil.musicInternalKeySpaceName+"."+MusicUtil.nodeIdsTable+" where public_ip='"+musicNodeIP+"';";
		ResultSet rs = get(query);
        List<Row> rows = rs.all();
        Row row = rows.get(0);//there should be only one row
        return row.getString("nodeId");
	}
*/	
	private static  void syncAllReplicas(String key){
		/*sync operation 		
		wait till all the eventual puts are done at the other music nodes (with timeout)
		and wait till all values are the same at the music nodes*/
		PropertiesReader prop = new PropertiesReader();
		String[] allPublicIps = prop.getAllPublicIps();
		String[] allIds = prop.getAllIds();
		
		logger.debug("In sync all replicas:public Ips of nodes:"+allPublicIps);
		boolean synced = false;
		int backOffFactor = 0; 
		long backOffTime = 50;
		while(!synced){
			MusicUtil.sleep(backOffTime*backOffFactor);
			backOffFactor = backOffFactor +1;		
			MusicDigest referenceDigest = getDigest(allPublicIps[0], allIds[0],key);//could be any of the nodes
			String referenceValue = referenceDigest.getVectorTs();
			boolean mismatch = false;
			for (int i=1; i < allPublicIps.length;i++){ 
				MusicDigest mg;
				String nodePublicIp="";
				String nodeId="";
				try {
					nodePublicIp = allPublicIps[i];
					nodeId = allIds[i];
					mg = getDigest(nodePublicIp,nodeId, key);
					if(mg == null){
						logger.debug("In sync all replicas:There is no digest from "+nodePublicIp+", move on");
						continue;
					}
				} catch (NoHostAvailableException e) {
					logger.debug("In sync all replicas: Node "+nodePublicIp+" not responding correctly...");
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

	public static  MusicDigest getDigest(String publicIp,String nodeId, String key){
		long startTime = System.currentTimeMillis();
		logger.debug("In getDigest:Trying to obtain message digest from node "+nodeId+" with IP:"+publicIp);
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKey = splitString[2];

		TableMetadata tableInfo = returnColumnMetadata(keyspaceName, tableName);

		String primKeyFieldName = tableInfo.getPrimaryKey().get(0).getName();

		String queryToGetVectorTS =  "SELECT vector_ts FROM "+keyspaceName+"."+tableName+ " WHERE "+primKeyFieldName+"='"+primaryKey+"';";

		ResultSet results =null;
		results = getDSHandle(publicIp).executeEventualGet(queryToGetVectorTS);

		String vectorTs=null;
		for (Row row : results) {
			vectorTs = row.getString("vector_ts");
		}
		if(vectorTs == null)
			vectorTs = "";
		
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = MusicUtil.evPutsTable+nodeId;
		String queryToGetEvPutStatus =  "SELECT status FROM "+metaKeyspaceName+"."+metaTableName+ " WHERE key"+"='"+key+"';";
		results = getDSHandle(publicIp).executeEventualGet(queryToGetEvPutStatus);
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

	private static boolean isKeyUnLocked(String lockName){
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
	
	public boolean createKeyspace(String keyspaceName, JsonKeySpace kspObject) throws Exception {
		return true;
	}
	public static  boolean eventualPut(String keyspaceName, String tableName, String primaryKey, String query){
		//do a cassandra write one on the meta table with status as in-progress
		PropertiesReader prop = new PropertiesReader();
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String evPutTrackerTable = MusicUtil.evPutsTable+prop.getMyId();
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

	public static  boolean criticalPut(String keyspaceName, String tableName, String primaryKey, String query, String lockId){
		try {
			MusicLockState mls = getLockingServiceHandle().getLockState(keyspaceName+"."+tableName+"."+primaryKey);
			if(mls.getLockHolder().equals(lockId)){
				String consistency = "atomic";
				getDSHandle().executePut(query,consistency);
				return true; 
			}
			else 
				return false; 
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static  ResultSet criticalGet(String keyspaceName, String tableName, String primaryKey, String query, String lockId){
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

	public static  ResultSet get(String query){
		ResultSet results = getDSHandle().executeEventualGet(query);
		return results;
	}
	
	public static  ResultSet quorumGet(String query){
		ResultSet results = getDSHandle().executeCriticalGet(query);
		return results;

	}
	
	public static  Map<String, HashMap<String, Object>> marshallResults(ResultSet results){
		Map<String, HashMap<String, Object>> marshalledResults = getDSHandle().marshalData(results);
		return marshalledResults;
	}
	
	public static  String whoseTurnIsIt(String lockName){
		String currentHolder = getLockingServiceHandle().whoseTurnIsIt("/"+lockName)+"";
		return currentHolder;
	}

	public static String getLockNameFromId(String lockId){
		StringTokenizer st = new StringTokenizer(lockId);
		String lockName = st.nextToken("$");
		return lockName;
	}
	
	public static  void  unLock(String lockId){
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

	public static  void deleteLock(String lockName){
		logger.info("Deleting lock for "+lockName);
		getLockingServiceHandle().deleteLock("/"+lockName);
	}

	//this is mainly for some  functions like keyspace creation etc which does not
	//really need the bells and whistles of Music locking. 
	public static  void generalPut(String query, String consistency) throws Exception{
			getDSHandle().executePut(query,consistency);
	}

	public static TableMetadata returnColumnMetadata(String keyspace, String tablename){
		return getDSHandle().returnColumnMetadata(keyspace, tablename);
	}
	
	public static String convertToSqlDataType(DataType type,Object valueObj){
		String value ="";
		switch (type.getName()) {
		case UUID:
			value = valueObj+"";
			break;
		case TEXT:
			String valueString = valueObj+"";
			valueString = valueString.replace("'", "''");
			value = "'"+valueString+"'";
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

	public static Object convertToActualDataType(DataType colType,Object valueObj) throws Exception{
		String valueObjString = valueObj+"";
		switch(colType.getName()){
		case UUID: 
			return UUID.fromString(valueObjString);
		case VARINT: 
			return BigInteger.valueOf(Long.parseLong(valueObjString));
		case BIGINT: 
			return Long.parseLong(valueObjString);
		case INT: 
			return Integer.parseInt(valueObjString);
		case FLOAT: 
			return Float.parseFloat(valueObjString);	
		case DOUBLE: 
			return Double.parseDouble(valueObjString);
		case BOOLEAN: 
			return Boolean.parseBoolean(valueObjString);
		case MAP: 
			return (Map<String,Object>)valueObj;
		default:
			return valueObjString;
		}
	}

	
	//utility function to parse json map into sql like string
	public static String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter){
		String sqlString="";
		int counter =0;
		for (Map.Entry<String, Object> entry : jMap.entrySet())
		{
			Object ot = entry.getValue();
			String value = ot+"";
			if(ot instanceof String){
				value = "'"+value.replace("'", "''")+"'";
			}
			sqlString = sqlString+"'"+entry.getKey()+"':"+ value+"";
			if(counter!=jMap.size()-1)
				sqlString = sqlString+lineDelimiter;
			counter = counter +1;
		}	
		return sqlString;	
	}
	
	public static void pureZkCreate(String nodeName){
		getLockingServiceHandle().getzkLockHandle().createNode(nodeName);
	}
	
	public static void pureZkWrite(String nodeName, byte[] data){
		getLockingServiceHandle().getzkLockHandle().setNodeData(nodeName, data);
	}

	public static byte[] pureZkRead(String nodeName){
		return getLockingServiceHandle().getzkLockHandle().getNodeData(nodeName);
	}
}
