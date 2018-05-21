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
import com.att.research.music.datastore.JsonKeySpace;
import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.main.MusicCore.Condition;
import com.att.research.music.zklockingservice.MusicLockState;
import com.att.research.music.zklockingservice.MusicLockingService;
import com.att.research.music.zklockingservice.MusicLockState.LockStatus;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicPureCassaCore {

	static  MusicDataStore   mDstoreHandle = null;
	final static Logger logger = Logger.getLogger(MusicPureCassaCore.class);


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
		String[] splitString = lockName.split("\\.");
		String keyspace = splitString[0];
		String table = splitString[1];
		String key = splitString[2];
		UUID lockReferenceUUID = getDSHandle().createLockReferenceUUID(keyspace, table, key);
		return "$"+lockName+"$"+lockReferenceUUID;
	}

	public static boolean isTableOrKeySpaceLock(String key){
		String[] splitString = key.split("\\.");
		if(splitString.length > 2)
			return false;
		else
			return true;
	}


	public static  WriteReturnType  acquireLock(String lockReference){
		try {
			String[] lockRefComponents = lockReference.split("\\$");
			String lockName = lockRefComponents[1];
			UUID lockReferenceUUID = UUID.fromString(lockRefComponents[2]);
			
			String[] lockNameComponents = lockName.split("\\.");
			String keyspace = lockNameComponents[0];
			String table = lockNameComponents[1];
			String key = lockNameComponents[2];


			Boolean result = getDSHandle().isItMyTurn(keyspace, table, key, lockReferenceUUID);
			if(result == false)
				return new WriteReturnType(ResultType.FAILURE,"You are NOT the lock holder"); 

			
			//check to see if the key is in an unsynced state
			String query = "select * from music_internal.unsynced_keys where key='"+key+"';";
			ResultSet results = getDSHandle().executeCriticalGet(query);
			if (results.all().size() != 0) {
				logger.info("In acquire lock: Since there was a forcible release, need to sync quorum!");
				syncQuorum(key);
				String cleanQuery = "delete * from music_internal.unsynced_keys where key='"+key+"';";
				getDSHandle().executePut(cleanQuery, "critical");
			}


			return new WriteReturnType(ResultType.SUCCESS,"You are the lock holder");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new WriteReturnType(ResultType.FAILURE,"Exception thrown while doing the acquire lock:\n"+exceptionAsString); 

		}
	}


	public static void createLockingTable(String keyspace, String table) {
		getDSHandle().createLockingTable(keyspace, table);
	}

	public static  WriteReturnType eventualPut(String query){
		getDSHandle().executePut(query, "eventual");
		//logger.info("Time taken for the actual eventual put:"+(end-start)+" ms");
		return new WriteReturnType(ResultType.SUCCESS,""); 

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

	public static WriteReturnType criticalPut(String keyspaceName, String tableName, String primaryKey, String query, String lockId, Condition conditionInfo){
		return criticalPut(keyspaceName, tableName, primaryKey, query, lockId, conditionInfo,1);
	}

	
	public static WriteReturnType criticalPut(String keyspaceName, String tableName, String primaryKey, String query, String lockReference, Condition conditionInfo, int batchSize){
		try {
			if(conditionInfo != null)//check if condition is true
				if(conditionInfo.testCondition() == false)
					return new WriteReturnType(ResultType.FAILURE,"Lock acquired but the condition is not true"); 
				for(int i =0; i < batchSize; ++i)
					getDSHandle().executePut(query,"critical");
				return new WriteReturnType(ResultType.SUCCESS,"Update performed"); 
		}catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new WriteReturnType(ResultType.FAILURE,"Exception thrown while doing the critical put, either you are not connected to a quorum or the condition is wrong:\n"+exceptionAsString); 
		}
	}


	public static WriteReturnType atomicPut(String keyspaceName, String tableName, String primaryKey, String query, Condition conditionInfo, int batchSize){
		long start = System.currentTimeMillis();
		String key = keyspaceName+"."+tableName+"."+primaryKey;
		String lockReference = createLockReference(key);
		long lockCreationTime = System.currentTimeMillis();
		WriteReturnType lockAcqResult = acquireLock(lockReference);
		long lockAcqTime = System.currentTimeMillis();
		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			WriteReturnType criticalPutResult = criticalPut(keyspaceName, tableName, primaryKey, query, lockReference,conditionInfo, batchSize);
			long criticalPutTime = System.currentTimeMillis();
			voluntaryReleaseLock(lockReference);
			long lockDeleteTime = System.currentTimeMillis();
			String timingInfo = "|lock creation time:"+(lockCreationTime-start)+"|lock accquire time:"+(lockAcqTime-lockCreationTime)+"|critical put time:"+(criticalPutTime-lockAcqTime)+"|lock release time:"+(lockDeleteTime-criticalPutTime)+"|";
			criticalPutResult.setTimingInfo(timingInfo);
			return criticalPutResult;
		}
		else{
			voluntaryReleaseLock(lockReference);
			return lockAcqResult;	
		}
	}

	public static  Map<String, HashMap<String, Object>> marshallResults(ResultSet results){
		Map<String, HashMap<String, Object>> marshalledResults = getDSHandle().marshalData(results);
		return marshalledResults;
	}

	public static  void  voluntaryReleaseLock(String lockReference){
		String[] lockRefComponents = lockReference.split("\\$");
		String lockName = lockRefComponents[1];
		UUID lockReferenceUUID = UUID.fromString(lockRefComponents[2]);
		
		String[] lockNameComponents = lockName.split("\\.");
		String keyspace = lockNameComponents[0];
		String table = lockNameComponents[1];
		String key = lockNameComponents[2];

		getDSHandle().releaseLock(keyspace, table, key, lockReferenceUUID);
	}

	public static TableMetadata returnColumnMetadata(String keyspace, String tablename){
		return getDSHandle().returnColumnMetadata(keyspace, tablename);
	}

	public static  ResultSet quorumGet(String query){
		ResultSet results = getDSHandle().executeCriticalGet(query);
		return results;

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


}
