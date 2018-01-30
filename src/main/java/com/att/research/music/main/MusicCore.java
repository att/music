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

import java.io.PrintWriter;
import java.io.StringWriter;
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
import com.att.research.music.lockingservice.MusicLockState.LockStatus;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicCore {

	static  MusicLockingService  mLockHandle = null;
	static  MusicDataStore   mDstoreHandle = null;
	final static Logger logger = Logger.getLogger(MusicCore.class);

	public static class Condition{
		Map<String, Object> conditions;
		String selectQueryForTheRow;
		public Condition(Map<String, Object> conditions, String selectQueryForTheRow) {
			this.conditions = conditions;
			this.selectQueryForTheRow = selectQueryForTheRow;
		} 	
		public boolean testCondition(){
			//first generate the row
			ResultSet results = quorumGet(selectQueryForTheRow);
			Row row = results.one();
			return getDSHandle().doesRowSatisfyCondition(row, conditions);
		}
	}

	public static MusicLockingService getLockingServiceHandle(){
		logger.debug("Acquiring lock store handle");
		long start = System.currentTimeMillis();
		if(mLockHandle == null){
			mLockHandle = new MusicLockingService();
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire lock store handle:"+(end-start)+" ms");
		return mLockHandle;
	}

	public static MusicDataStore getDSHandle(String remoteIp){
		logger.debug("Acquiring data store handle");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore(remoteIp);
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire data store handle:"+(end-start)+" ms");
		return mDstoreHandle;
	}                                                                            

	public static MusicDataStore getDSHandle(){
		logger.debug("Acquiring data store handle");
		long start = System.currentTimeMillis();
		if(mDstoreHandle == null){
			mDstoreHandle = new MusicDataStore();
		}
		long end = System.currentTimeMillis();
		logger.debug("Time taken to acquire data store handle:"+(end-start)+" ms");
		return mDstoreHandle;
	}        

	public static  String createLockReference(String lockName){
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


	public static  ReturnType  acquireLock(String key, String lockId){
		Boolean result = getLockingServiceHandle().isMyTurn(lockId);	
		if(result == false){
			return new ReturnType(ResultType.FAILURE,"You are the not the lock holder"); 
		}

		//check to see if the key is in an unsynced state
		String query = "select * from music_internal.unsynced_keys where key='"+key+"';";
		ResultSet results = getDSHandle().executeCriticalGet(query);
		if (results.all().size() != 0) {
			logger.info("In acquire lock: Since there was a forcible release, need to sync quorum!");
			syncQuorum(key);
			String cleanQuery = "delete * from music_internal.unsynced_keys where key='"+key+"';";
			getDSHandle().executePut(cleanQuery, "critical");
		}

		return new ReturnType(ResultType.SUCCESS,"You are the lock holder"); 
	}


	public boolean createKeyspace(String keyspaceName, JsonKeySpace kspObject) throws Exception {
		return true;
	}

	public static  ReturnType eventualPut(String query){
		long start = System.currentTimeMillis();
		getDSHandle().executePut(query, "eventual");
		long end = System.currentTimeMillis();
		logger.info("Time taken for the actual eventual put:"+(end-start)+" ms");
		return new ReturnType(ResultType.SUCCESS,""); 

	}

	private static void syncQuorum(String key){
		logger.info("Performing sync operation---");
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKeyValue = splitString[2];

		//get the primary key d
		TableMetadata tableInfo = returnColumnMetadata(keyspaceName, tableName);
		String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();//we only support single primary key
		DataType primaryKeyType = tableInfo.getPrimaryKey().get(0).getType();
		String cqlFormattedPrimaryKeyValue = convertToCQLDataType(primaryKeyType, primaryKeyValue);

		//get the row of data from a quorum
		String selectQuery =  "SELECT *  FROM "+keyspaceName+"."+tableName+ " WHERE "+primaryKeyName+"="+cqlFormattedPrimaryKeyValue+";"; 
		ResultSet results = getDSHandle().executeCriticalGet(selectQuery);

		//write it back to a quorum
		Row row = results.one();
		ColumnDefinitions colInfo = row.getColumnDefinitions();
		int totalColumns = colInfo.size();
		int counter =1;
		String fieldValueString="";
		for (Definition definition : colInfo){
			String colName = definition.getName();
			if(colName.equals(primaryKeyName))
				continue; 
			DataType colType = definition.getType();
			Object valueObj = getDSHandle().getColValue(row, colName, colType);	
			String valueString = convertToCQLDataType(colType,valueObj);	
			fieldValueString = fieldValueString+ colName+"="+valueString;
			if(counter!=(totalColumns-1))
				fieldValueString = fieldValueString+",";
			counter = counter +1;
		}

		String updateQuery =  "UPDATE "+keyspaceName+"."+tableName+" SET "+fieldValueString+" WHERE "+primaryKeyName+"="+cqlFormattedPrimaryKeyValue+";";
		getDSHandle().executePut(updateQuery, "critical");
	}

	public static ReturnType criticalPut(String keyspaceName, String tableName, String primaryKey, String query, String lockId, Condition conditionInfo){

		Boolean result = getLockingServiceHandle().isMyTurn(lockId);	
		if(result == false)
			return new ReturnType(ResultType.FAILURE,"Cannot perform operation since you are the not the lock holder"); 

		if(conditionInfo != null)//check if condition is true
			if(conditionInfo.testCondition() == false)
				return new ReturnType(ResultType.FAILURE,"Lock acquired but the condition is not true"); 

		try {
			getDSHandle().executePut(query,"critical");
			return new ReturnType(ResultType.SUCCESS,"Update performed"); 
		}catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ReturnType(ResultType.FAILURE,"Exception thrown while doing the critical put, either you are not connected to a quorum or the condition is wrong:\n"+exceptionAsString); 
		}
	}

	public static ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey, String query, Condition conditionInfo){
		long start = System.currentTimeMillis();
		String key = keyspaceName+"."+tableName+"."+primaryKey;
		String lockId = createLockReference(key);
		long lockCreationTime = System.currentTimeMillis();
		ReturnType lockAcqResult = acquireLock(key, lockId);
		long lockAcqTime = System.currentTimeMillis();
		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			ReturnType criticalPutResult = criticalPut(keyspaceName, tableName, primaryKey, query, lockId,conditionInfo);
			long criticalPutTime = System.currentTimeMillis();
			voluntaryReleaseLock(lockId);
			long lockDeleteTime = System.currentTimeMillis();
			String timingInfo = "|lock creation time:"+(lockCreationTime-start)+"|lock accquire time:"+(lockAcqTime-lockCreationTime)+"|critical put time:"+(criticalPutTime-lockAcqTime)+"|lock release time:"+(lockDeleteTime-criticalPutTime)+"|";
			criticalPutResult.setTimingInfo(timingInfo);
			return criticalPutResult;
		}
		else{
			destroyLockRef(lockId);
			return lockAcqResult;	
		}
	}


	//this function is mainly for the benchmarks to see the effect of lock deletion.
	public static ReturnType atomicPutWithDeleteLock(String keyspaceName, String tableName, String primaryKey, String query, Condition conditionInfo){
		long start = System.currentTimeMillis();
		String key = keyspaceName+"."+tableName+"."+primaryKey;
		String lockId = createLockReference(key);
		long lockCreationTime = System.currentTimeMillis();
		ReturnType lockAcqResult = acquireLock(key, lockId);
		long lockAcqTime = System.currentTimeMillis();
		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			ReturnType criticalPutResult = criticalPut(keyspaceName, tableName, primaryKey, query, lockId,conditionInfo);
			long criticalPutTime = System.currentTimeMillis();
			deleteLock(key);
			long lockDeleteTime = System.currentTimeMillis();
			String timingInfo = "|lock creation time:"+(lockCreationTime-start)+"|lock accquire time:"+(lockAcqTime-lockCreationTime)+"|critical put time:"+(criticalPutTime-lockAcqTime)+"|lock delete time:"+(lockDeleteTime-criticalPutTime)+"|";
			criticalPutResult.setTimingInfo(timingInfo);
			return criticalPutResult;
		}
		else{
			deleteLock(key);
			return lockAcqResult;	
		}
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

	public static void destroyLockRef(String lockId){
		long start = System.currentTimeMillis();
		getLockingServiceHandle().unlockAndDeleteId(lockId);
		long end = System.currentTimeMillis();			
		logger.info("Time taken to destroy lock reference:"+(end-start)+" ms");
	}

	public static  void  voluntaryReleaseLock(String lockId){
		getLockingServiceHandle().unlockAndDeleteId(lockId);
	}

	public static  void  forcedReleaseLock(String lockId, boolean voluntaryRelease){
		getLockingServiceHandle().unlockAndDeleteId(lockId);
	}

	public static  void deleteLock(String lockName){
		getLockingServiceHandle().deleteLock("/"+lockName);
	}

	//this is mainly for some  functions like keyspace creation etc which does not
	//really need the bells and whistles of Music locking. 
	public static  void nonKeyRelatedPut(String query, String consistency) throws Exception{
		getDSHandle().executePut(query,consistency);
	}

	public static TableMetadata returnColumnMetadata(String keyspace, String tablename){
		return getDSHandle().returnColumnMetadata(keyspace, tablename);
	}

	public static String convertToCQLDataType(DataType type,Object valueObj){
		String value ="";
		switch (type.getName()) {
		case UUID:
			value = valueObj+"";
			break;
		case TEXT: case VARCHAR:
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

	public static ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey, String query){
		String key = keyspaceName+"."+tableName+"."+primaryKey;
		String lockId = createLockReference(key);
		ReturnType lockAcqResult = acquireLock(key, lockId);
		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			logger.info("acquired lock with id "+lockId);
			ResultSet result = criticalGet(keyspaceName, tableName, primaryKey, query, lockId);
			voluntaryReleaseLock(lockId);
			return result;
		}
		else{
			logger.info("unable to acquire lock, id "+lockId);
			return null; 	
		}
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

	public static void pureZkCreate(String nodeName){
		getLockingServiceHandle().getzkLockHandle().createNode(nodeName);
	}

	public static void pureZkWrite(String nodeName, byte[] data){
		long start = System.currentTimeMillis();
		getLockingServiceHandle().getzkLockHandle().setNodeData(nodeName, data);
		logger.info("Performed zookeeper write to "+nodeName);
		long end = System.currentTimeMillis();
		//logger.info("Time taken for the actual zk put:"+(end-start)+" ms");
	}

	public static byte[] pureZkRead(String nodeName){
		long start = System.currentTimeMillis();
		byte[] data = getLockingServiceHandle().getzkLockHandle().getNodeData(nodeName);
		long end = System.currentTimeMillis();
		logger.info("Time taken for the actual zk put:"+(end-start)+" ms");
		return data;
	}
}
