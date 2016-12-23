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
import java.util.Map;
import java.util.StringTokenizer;

import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.datastore.jsonobjects.RestMusicFunctions;
import com.att.research.music.lockingservice.MusicLockingService;
import com.att.research.music.lockingservice.ZkStatelessLockService;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicCore {

	static  MusicLockingService  mLockHandle = null;
	static  MusicDataStore   mDstoreHandle = null;

	public static MusicLockingService getLockingServiceHandle(){
		if(MusicUtil.debug) System.out.println("Trying to acquire locking service handle..");
		long start = System.currentTimeMillis();
		if(mLockHandle == null){
			mLockHandle = new MusicLockingService();
		}
		long end = System.currentTimeMillis();
		if(MusicUtil.debug) System.out.println("Time taken to get zookeeper handle:"+ (end-start));
		return mLockHandle;
	}

	public static MusicDataStore getDSHandle(String remoteIp){
		if(MusicUtil.debug) System.out.println("Trying to acquire ds handle..");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore(remoteIp);
		}
		long end = System.currentTimeMillis();
		if(MusicUtil.debug) System.out.println("Time taken to get cassandra handle:"+ (end-start));
		return mDstoreHandle;
	}                                                                            


	public static MusicDataStore getDSHandle(){
		if(MusicUtil.debug) System.out.println("Trying to acquire ds handle..");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore();
		}
		long end = System.currentTimeMillis();
		if(MusicUtil.debug) System.out.println("Time taken to get cassandra handle:"+ (end-start));
		return mDstoreHandle;
	}                                                                            

	public static  String createLockReference(String lockName){
		if(MusicUtil.debug) System.out.println("In music core create lock reference..");
		String lockId = getLockingServiceHandle().createLockId("/"+lockName);
	//	getLockingServiceHandle().close();
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
		if(MusicUtil.debug) System.out.println("No lock object exists as of now..");
	}
		return null;
	}

	public static  boolean  acquireLock(String key, String lockId){
		/* first check if I am on top. Since ids are not reusable there is no need to check lockStatus
		 * If the status is unlocked, then the above
		call will automatically return false.*/
		if(MusicUtil.debug) System.out.println("In acquire lock....");
		Boolean result = getLockingServiceHandle().isMyTurn(lockId);	
		if(result == false){
			if(MusicUtil.debug) System.out.println("In acquire lock: Not your turn, someone else has the lock");
			return false;
		}
		
		
		//this is for backward compatibility where locks could also be acquired on just
		//keyspaces or tables.
		if(isTableOrKeySpaceLock(key) == true){
			if(MusicUtil.debug) System.out.println("In acquire lock: A table or keyspace lock is no longer relevant, so returning true");
			return false; 
		}
		
		//if you are already the lock holder no need to sync, simply return true
		//read the lock name corresponding to the key and if the status is locked or being locked, then return false
		try{
			String currentLockHolder = getMusicLockState(key).getLockHolder();
			if(lockId.equals(currentLockHolder)){
				if(MusicUtil.debug) System.out.println("In acquire lock: You already have the lock!");
				return true;
			}
		}catch (NullPointerException e) {
			if(MusicUtil.debug) System.out.println("No lock object exists as of now..");
		}
		
		//change status to "being locked". This state transition is necessary to ensure syncing before granting the lock
		String lockHolder = null;
		MusicLockState mls = new MusicLockState(MusicLockState.LockStatus.BEING_LOCKED, lockHolder);
		getLockingServiceHandle().setLockState(key, mls);
		if(MusicUtil.debug) System.out.println("In acquire lock: Set lock state to being_locked");
		
		mls = new MusicLockState(MusicLockState.LockStatus.LOCKED, lockHolder);
		getLockingServiceHandle().setLockState(key, mls);
		if(MusicUtil.debug) System.out.println("In acquire lock: Set lock state to locked");

		//ensure that there are no eventual puts pending and that all replicas of key
		//have the same value
		syncAllReplicas(key);
		if(MusicUtil.debug) System.out.println("In acquire lock: synced all replicas");


		//change status to locked
		lockHolder = lockId;
		mls = new MusicLockState(MusicLockState.LockStatus.LOCKED, lockHolder);
		getLockingServiceHandle().setLockState(key, mls);
		if(MusicUtil.debug) System.out.println("In acquire lock: Set lock state to locked and assigned current lock ref "+ lockId+" as holder");
//		getLockingServiceHandle().close();
		return result;
	}

	private static  void syncAllReplicas(String key){
		/*sync operation 		
		wait till all the eventual puts are done at the other music nodes (with timeout)
		and wait till all values are the same at other music nodes*/
		ArrayList<String> listOfNodePublicIps = MusicUtil.getOtherMusicPublicIps();
		ArrayList<String> listOfNodeIds = MusicUtil.getOtherMusicNodeIds();

		boolean synced = false;
		int backOffFactor = 0; 
		long backOffTime = 50;
		while(!synced){
			MusicUtil.sleep(backOffTime*backOffFactor);
			backOffFactor = backOffFactor +1;
			long start = System.currentTimeMillis();
			MusicDigest localDigest = getLocalDigest(key);
		//	String localValue = getDigest(MusicUtil.getMyId(),MusicUtil.getMyPublicIp(),key).getVectorTs();
			String localValue = localDigest.getVectorTs();
			long end = System.currentTimeMillis();
			if(MusicUtil.debug) System.out.println("In sync: time to get local digest:"+(end-start));
			if(MusicUtil.debug) System.out.println("Local digest:"+localValue);
			boolean mismatch = false;
			for (int i=0; i < listOfNodeIds.size();i++){ 
				MusicDigest mg;
				try {
					String remoteNodeId = listOfNodeIds.get(i);
					String remoteNodePublicIp = listOfNodePublicIps.get(i);
					if(MusicUtil.debug) System.out.println("Trying to get Message digest from node "+remoteNodeId+" with ip:"+ remoteNodePublicIp);
					start = System.currentTimeMillis();
					mg = getRemoteDigest(remoteNodePublicIp, key);
					end = System.currentTimeMillis();
					if(MusicUtil.debug) System.out.println("In sync: time to get remote digest from node "+remoteNodeId+" is "+(end-start));
					if(mg == null){
						if(MusicUtil.debug) System.out.println("There is no digest, move on");
						continue;
					}
					if(MusicUtil.debug) System.out.println("Received digest ["+mg+"]  from node "+remoteNodeId+" with ip:"+ remoteNodePublicIp);
				} catch (NoHostAvailableException e) {
					if(MusicUtil.debug) System.out.println("Node not responding correctly...");
					continue;//if the host is dead we do not care about his value
				}
				if(mg.getEvPutStatus().equals("inprogress")){	
					mismatch = true;
					break;
				}
				if(localValue.equals(mg.getVectorTs()) == false){
					mismatch = true;
					break;
				}
			}
			if(mismatch == false)
				synced = true; 		
		}
	}

/*	private static  MusicDigest getDigest(String nodeId, String publicIp, String key) throws com.datastax.driver.core.exceptions.NoHostAvailableException{
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKey = splitString[2];

		long start  = System.currentTimeMillis();
		RestMusicFunctions restHandle = new RestMusicFunctions(publicIp);

		TableMetadata tableInfo = returnColumnMetadata(keyspaceName, tableName);

		String primKeyFieldName = tableInfo.getPrimaryKey().get(0).getName();
		primaryKey = "'"+primaryKey+"'";
		Map<String,Object> dataRow = restHandle.readSpecificRow(keyspaceName, tableName, primKeyFieldName, primaryKey);
		long end = System.currentTimeMillis();
		if(MusicUtil.debug) System.out.println("In get digest: time to get remote table data from node "+nodeId+" is "+(end-start));
		if(MusicUtil.debug) System.out.println("In get digest, data row:"+dataRow);

		start  = System.currentTimeMillis();
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = "EvPutStatusAt"+nodeId;
		key = "'"+key+"'";
		Map<String,Object> evPutStatusRow = restHandle.readSpecificRow(metaKeyspaceName, metaTableName, "key", key);
		if(MusicUtil.debug) System.out.println("In get digest, evputstatus row:"+evPutStatusRow);
		end = System.currentTimeMillis();
		if(MusicUtil.debug) System.out.println("In get digest: time to get evput data from node "+nodeId+" is "+(end-start));
		String vector_ts = (String)dataRow.get("vector_ts");
		String evPutStatus;
		if(evPutStatusRow == null)
			evPutStatus = "";
		else 
			evPutStatus = (String)evPutStatusRow.get("status");
		MusicDigest mg = new MusicDigest(evPutStatus,vector_ts);	
		return mg;


		String queryToGetVectorTS =  "SELECT vector_ts FROM "+keyspaceName+"."+tableName+ " WHERE "+rowSpec+";";

		ResultSet results =null;
		results = MusicCore.get(publicIp,queryToGetVectorTS);
		String vectorTs=null;
		for (Row row : results) {
			vectorTs = row.getString("vector_ts");
		}

		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = "EvPutStatusAt"+nodeId;
		String queryToGetEvPutStatus =  "SELECT status FROM "+metaKeyspaceName+"."+metaTableName+ " WHERE key"+"='"+primaryKey+"';";
		results = MusicCore.get(publicIp,queryToGetEvPutStatus);
		String evPutStatus =null;
		for (Row row : results) {
			evPutStatus = row.getString("status");
		}
		
	}
*/	
/*	public static  MusicDigest getRemoteDigest(String publicIp, String key){
		RestMusicFunctions restHandle = new RestMusicFunctions(publicIp);
		Map<String, String> digestMap = restHandle.getMusicDigest(key);
		if(MusicUtil.debug) System.out.println(digestMap);
		MusicDigest mg = new MusicDigest(digestMap.get("evPutStatus"),digestMap.get("vectorTs"));	
		return mg;
	}
*/	
	public static  MusicDigest getRemoteDigest(String publicIp, String key){
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKey = splitString[2];

		TableMetadata tableInfo = returnColumnMetadata(keyspaceName, tableName);

		String primKeyFieldName = tableInfo.getPrimaryKey().get(0).getName();

		String queryToGetVectorTS =  "SELECT vector_ts FROM "+keyspaceName+"."+tableName+ " WHERE "+primKeyFieldName+"='"+primaryKey+"';";
		if(MusicUtil.debug) System.out.println(queryToGetVectorTS);

		ResultSet results =null;
		results = getDSHandle(publicIp).executeGetQuery(queryToGetVectorTS);

		String vectorTs=null;
		for (Row row : results) {
			vectorTs = row.getString("vector_ts");
		}
		if(vectorTs == null)
			vectorTs = "";
		if(MusicUtil.debug) System.out.println(vectorTs);
		
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = "EvPutStatusAt"+MusicUtil.getMyId();
		String queryToGetEvPutStatus =  "SELECT status FROM "+metaKeyspaceName+"."+metaTableName+ " WHERE key"+"='"+key+"';";
		if(MusicUtil.debug) System.out.println(queryToGetEvPutStatus);
		results = getDSHandle(publicIp).executeGetQuery(queryToGetEvPutStatus);
		String evPutStatus =null;
		for (Row row : results) {
			evPutStatus = row.getString("status");
		}
		if(evPutStatus == null)
			evPutStatus = "";
		if(MusicUtil.debug) System.out.println(evPutStatus);
		MusicDigest mg = new MusicDigest(evPutStatus,vectorTs);	
		return mg;
	}

	public static  MusicDigest getLocalDigest(String key){
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKey = splitString[2];

		TableMetadata tableInfo = returnColumnMetadata(keyspaceName, tableName);

		String primKeyFieldName = tableInfo.getPrimaryKey().get(0).getName();

		String queryToGetVectorTS =  "SELECT vector_ts FROM "+keyspaceName+"."+tableName+ " WHERE "+primKeyFieldName+"='"+primaryKey+"';";
		if(MusicUtil.debug) System.out.println(queryToGetVectorTS);

		ResultSet results =null;
		results = MusicCore.get(queryToGetVectorTS);
		String vectorTs=null;
		for (Row row : results) {
			vectorTs = row.getString("vector_ts");
		}
		if(vectorTs == null)
			vectorTs = "";
		if(MusicUtil.debug) System.out.println(vectorTs);
		
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = "EvPutStatusAt"+MusicUtil.getMyId();
		String queryToGetEvPutStatus =  "SELECT status FROM "+metaKeyspaceName+"."+metaTableName+ " WHERE key"+"='"+key+"';";
		if(MusicUtil.debug) System.out.println(queryToGetEvPutStatus);
		results = MusicCore.get(queryToGetEvPutStatus);
		String evPutStatus =null;
		for (Row row : results) {
			evPutStatus = row.getString("status");
		}
		if(evPutStatus == null)
			evPutStatus = "";
		if(MusicUtil.debug) System.out.println(evPutStatus);
		MusicDigest mg = new MusicDigest(evPutStatus,vectorTs);	
		return mg;
	}

	private static boolean isKeyUnLocked(String lockName){
		try {
			MusicLockState mls;
			mls = getLockingServiceHandle().getLockState(lockName);
			if(mls.getLockStatus().equals(MusicLockState.LockStatus.UNLOCKED) == false){
				if(MusicUtil.debug) System.out.println("The key is locked");
				return false;//someone else is holding the lock to the key
			}
		} catch (NullPointerException e) {
			if(MusicUtil.debug) System.out.println("No lock has been created for this, so go ahead with the eventual put..");
		}
		return true;
	}
	
	public static  boolean eventualPut(String keyspaceName, String tableName, String primaryKey, String query){
		//do a cassandra write one on the meta table with status as attempting
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = "EvPutStatusAt"+MusicUtil.getMyId();
		String metaKey = "'"+keyspaceName+"."+tableName+"."+primaryKey+"'";

		String queryToUpdateEvPut =  "Insert into "+metaKeyspaceName+"."+metaTableName+"  (key,status) values ("+metaKey+",'inprogress');";   
		if(MusicUtil.debug) System.out.println(queryToUpdateEvPut);
		getDSHandle().executePutQuery(queryToUpdateEvPut, "eventual");

		boolean result; 
		String lockName = keyspaceName+"."+tableName+"."+primaryKey;
		if(isKeyUnLocked(lockName) == false){
			result = false;
		}else{
			if(MusicUtil.debug) System.out.println("The key is un-locked, CAN perform eventual puts..");
			//do actual write 
			if(MusicUtil.debug) System.out.println(query);
			getDSHandle().executePutQuery(query, "eventual");
			result = true;
		}

		//clean up meta table
		String queryToResetEvPutStatus =  "Delete from "+metaKeyspaceName+"."+metaTableName+" where key="+metaKey+";";   
		if(MusicUtil.debug) System.out.println(queryToResetEvPutStatus);
		getDSHandle().executePutQuery(queryToResetEvPutStatus, "eventual");
		return result;
	}

	public static  boolean criticalPut(String keyspaceName, String tableName, String primaryKey, String query, String lockId){
		try {
			MusicLockState mls = getLockingServiceHandle().getLockState(keyspaceName+"."+tableName+"."+primaryKey);
			if(mls.getLockHolder().equals(lockId)){
				String consistency = "atomic";
				getDSHandle().executePutQuery(query,consistency);
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
				results = getDSHandle().executeCriticalGetQuery(query);
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
		ResultSet results = getDSHandle().executeGetQuery(query);
		return results;
	}
	
	public static  ResultSet quorumGet(String query){
		ResultSet results = getDSHandle().executeCriticalGetQuery(query);
		return results;

	}
	
	public static  Map<String, HashMap<String, Object>> marshallResults(ResultSet results){
		Map<String, HashMap<String, Object>> marshalledResults = getDSHandle().marshalData(results);
		return marshalledResults;
	}
	
	public static  String whoseTurnIsIt(String lockName){
		String currentHolder = getLockingServiceHandle().whoseTurnIsIt("/"+lockName)+"";
//		getLockingServiceHandle().close();
		return currentHolder;
	}

	public static String getLockNameFromId(String lockId){
		StringTokenizer st = new StringTokenizer(lockId);
		String lockName = st.nextToken("$");
		return lockName;
	}
	public static  void  unLock(String lockId){
		if(MusicUtil.debug) System.out.println("In the Formal release, lock id is:"+lockId);
		getLockingServiceHandle().unlockAndDeleteId(lockId);
		MusicLockState mls;
		String lockName = getLockNameFromId(lockId);
		String nextInLine = whoseTurnIsIt(lockName);
		if(MusicUtil.debug) System.out.println("In the Formal release, next in line lock id is:"+nextInLine);
		if(!nextInLine.equals("")){
			if(MusicUtil.debug) System.out.println("In the Formal release, since someone is in line, give him the lock");
			mls = new MusicLockState(MusicLockState.LockStatus.LOCKED, nextInLine);
		}
		else{
			//change status to unlocked
			if(MusicUtil.debug) System.out.println("In the Formal release, no one in line");
			mls = new MusicLockState(MusicLockState.LockStatus.UNLOCKED, null);
		}
		getLockingServiceHandle().setLockState(lockName, mls);
//		getLockingServiceHandle().close();
	}

	public static  void deleteLock(String lockName){
		getLockingServiceHandle().deleteLock("/"+lockName);
	//	getLockingServiceHandle().close();
	}

	//this is mainly for some  functions like keyspace creation etc which does not
	//really need the bells and whistles of Music locking. 
	public static  void generalPut(String query, String consistency){
		if(MusicUtil.debug) System.out.println("In music core, executing general put..");
		try {
			getDSHandle().executePutQuery(query,consistency);
		//	getDSHandle(MusicUtil.myCassaHost).close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	public static String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter){
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
		//if(MusicUtil.debug) System.out.println(sqlString);
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

	public static void main(String[] args){
		
		MusicCore.getLocalDigest("testks.employees.bharath");
		
		MusicCore.getRemoteDigest("localhost", "testks.employees.bharath");
		
/*		String nodeId ="1";
		String publicIp = "135.197.226.99";
		String key = "testks.employees.bharath";
		if(MusicUtil.debug) System.out.println("hi");
		
		MusicCore.getDigest(nodeId, publicIp, key);
*//*		RestMusicFunctions restHandle = new RestMusicFunctions(publicIp);

		Map<String,Object> dataRow = restHandle.readSpecificRow("testks", "employees", "emp_name","bharath");
		
		String metaKeyspaceName = MusicUtil.musicInternalKeySpaceName;
		String metaTableName = "EvPutStatusAt"+nodeId;

		Map<String,Object> evPutStatusRow = restHandle.readSpecificRow(metaKeyspaceName, metaTableName, "key", key);

		if(MusicUtil.debug) System.out.println(dataRow);
		if(MusicUtil.debug) System.out.println(evPutStatusRow);
		*/
		

		
	}


}
