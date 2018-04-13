package com.att.research.music.conditionals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.lockingservice.MusicLockState;
import com.att.research.music.main.MusicCore;
import com.att.research.music.main.MusicUtil;
import com.att.research.music.main.ResultType;
import com.att.research.music.main.ReturnType;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.att.research.music.conditionals.*;

public class MusicConditionalCore {

	final static Logger logger = Logger.getLogger(MusicConditionalCore.class);

	public static ReturnType conditionalInsert(Map<String, String> queries, String tableName, String keySpace,
			String primaryKey) {

		long start = System.currentTimeMillis();
		String key = keySpace + "." + tableName + "." + primaryKey;
		String lockId = MusicCore.createLockReference(key);
		long leasePeriod = MusicUtil.defaultLockLeasePeriod;
		ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);
		try {
			if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				logger.info("acquired lock with id " + lockId);
				ReturnType criticalPutResult = conditionalInsertCriticalPut(keySpace, tableName, primaryKey, queries,
						lockId);
				boolean voluntaryRelease = true;
				MusicCore.releaseLock(lockId, voluntaryRelease);
				long end = System.currentTimeMillis();
				logger.info("Time taken for the atomic put:" + (end - start) + " ms");
				return criticalPutResult;
			} else {
				logger.info("unable to acquire lock, id " + lockId);
				MusicCore.destroyLockRef(lockId);
				return lockAcqResult;
			}
		} catch (Exception e) {
			MusicCore.destroyLockRef(lockId);
			return new ReturnType(ResultType.FAILURE, e.getMessage());
		}
	}

	public static ReturnType conditionalInsertCriticalPut(String keyspaceName, String tableName, String primaryKey,
			Map<String, String> query, String lockId) {
		long start = System.currentTimeMillis();
		ResultSet results = null;
		try {
			MusicLockState mls = MusicCore.getLockingServiceHandle()
					.getLockState(keyspaceName + "." + tableName + "." + primaryKey);
			if (mls.getLockHolder().equals(lockId) == true) {
				results = MusicCore.getDSHandle().executeCriticalGet(query.get("select"));
				if (results.all().isEmpty()) {
					MusicCore.getDSHandle().executePut(query.get("insert"), "critical");
					return new ReturnType(ResultType.SUCCESS, "insert");
				} else {
					MusicCore.getDSHandle().executePut(query.get("update"), "critical");
					MusicCore.getDSHandle().executePut(query.get("upsert"), "critical");
					return new ReturnType(ResultType.SUCCESS, "update");
				}
			} else
				return new ReturnType(ResultType.FAILURE,
						"Cannot perform operation since you are the not the lock holder");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ReturnType(ResultType.FAILURE,
					"Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
							+ exceptionAsString);
		}

	}

	public static ReturnType conditionalUpdate(Map<String, String> query, String tableName, String keySpace,
			String primaryKey, Map<String, String> changeOfStatus,String cascadeColumnName,String primaryKeyName,String planId) {

		long start = System.currentTimeMillis();
		String key = keySpace + "." + tableName + "." + primaryKey;
		String lockId = MusicCore.createLockReference(key);
		long leasePeriod = MusicUtil.defaultLockLeasePeriod;
		ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);
		try {
			if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				logger.info("acquired lock with id " + lockId);
				ReturnType criticalPutResult = conditionalUpdateCriticalPut(keySpace, tableName, primaryKey, query,
						lockId, changeOfStatus, cascadeColumnName,primaryKeyName,planId);
				boolean voluntaryRelease = true;
				MusicCore.releaseLock(lockId, voluntaryRelease);
				long end = System.currentTimeMillis();
				logger.info("Time taken for the atomic put:" + (end - start) + " ms");
				return criticalPutResult;
			} else {
				logger.info("unable to acquire lock, id " + lockId);
				MusicCore.destroyLockRef(lockId);
				return lockAcqResult;
			}
		} catch (Exception e) {
			MusicCore.destroyLockRef(lockId);
			return new ReturnType(ResultType.FAILURE, e.getMessage());
		}
	}

	public static ReturnType conditionalUpdateCriticalPut(String keyspaceName, String tableName, String primaryKey,
			Map<String, String> query, String lockId,Map<String, String> changeOfStatus,String cascadeColumnName,String primaryKeyname,String planId) {
		//have an object that contains all the fields instead of sending way too many params.
		long start = System.currentTimeMillis();
		ResultSet results = null;
		Row row = null;
		Map<String,String> updatedValues = new HashMap<String,String>();
		String updateQuery = "";
		try {
			MusicLockState mls = MusicCore.getLockingServiceHandle()
					.getLockState(keyspaceName + "." + tableName + "." + primaryKey);
			if (mls.getLockHolder().equals(lockId) == true) {
				results = MusicCore.getDSHandle().executeCriticalGet(query.get("select"));
				row = results.one();
				if (row != null) {
					if(planId == null || planId.isEmpty() || planId.length()==0)
					updatedValues = cascadeColumnUpdateAll(row, changeOfStatus, cascadeColumnName);//remove this function.just for testing
					else
					updatedValues = cascadeColumnUpdateSpecific(row, changeOfStatus, cascadeColumnName,planId);
					ColumnDefinitions colInfo = row.getColumnDefinitions();
					DataType colType = colInfo.getType(cascadeColumnName);
					updateQuery = getUpdateQuery(updatedValues, tableName, keyspaceName, primaryKey, cascadeColumnName, colType, primaryKeyname);
					MusicCore.getDSHandle().executePut(updateQuery, "critical");
					return new ReturnType(ResultType.SUCCESS, updatedValues.toString());
					
				} else {
				
					return new ReturnType(ResultType.FAILURE, "update failed");
				}
			} else
				return new ReturnType(ResultType.FAILURE,
						"Cannot perform operation since you are the not the lock holder");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ReturnType(ResultType.FAILURE,
					"Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
							+ exceptionAsString);
		}

	}
	//TODO
	//remove this function. just for test
	@SuppressWarnings("unchecked")
	public static Map<String,String> cascadeColumnUpdateAll (Row row,Map<String,String>changeOfStatus,String cascadeColumnName) {
		
		ColumnDefinitions colInfo = row.getColumnDefinitions();
		DataType colType = colInfo.getType(cascadeColumnName);
		Map<String,String> values =new HashMap<>();
		Object columnValue = getColValue(row, cascadeColumnName, colType);
		
		Map<String,String> finalValues =new HashMap<>();
	    values  =  (Map<String, String>) columnValue;
	    Map<String,String> keyValuePair = new HashMap<>();
	    for(String s:values.keySet()) {
	    	String temp = values.get(s);
	    	String temp2 = temp.replaceAll("\\{", "").replaceAll("\\}", "");
	    	String[] elements = temp2.split(",");
	    	for(String str:elements) {
	    		String[] keyValue = str.split("=");
	    		for(String key : changeOfStatus.keySet()) {
	    			
	    			if(keyValue[1].equalsIgnoreCase(key)) {
	    				keyValue[1] = changeOfStatus.get(key);
	    			}
	    			String tempKeytwo = keyValue[0].replaceAll("\\s", "");
	    			if(tempKeytwo.equalsIgnoreCase(key)) {
	    				keyValue[1] = changeOfStatus.get(key);
	    			}
	    		}
	    		String finalStatusKey = keyValue[0].replaceAll("\\s", "");
	    		String finalStatusValue = keyValue[1].replaceAll("\\s", "");
	    		keyValuePair.put(finalStatusKey, finalStatusValue);
	    		finalValues.put(s, keyValuePair.toString());
	    	}
	    }
	    return finalValues;
		
		
	}
	
public static Map<String,String> cascadeColumnUpdateSpecific (Row row,Map<String,String>changeOfStatus,String cascadeColumnName,String planId) {
		
		ColumnDefinitions colInfo = row.getColumnDefinitions();
		DataType colType = colInfo.getType(cascadeColumnName);
		Map<String,String> values =new HashMap<>();
		Object columnValue = getColValue(row, cascadeColumnName, colType);
		
		Map<String,String> finalValues =new HashMap<>();
	    values  =  (Map<String, String>) columnValue;
	    Map<String,String> keyValuePair = new HashMap<>();
	    if(values.keySet().contains(planId)) {
	    	String valueString = values.get(planId);
	    	String tempValueString = valueString.replaceAll("\\{", "").replaceAll("\\}", "");
	    	String[] elements = tempValueString.split(",");
	    	for(String str : elements) {
	    		String[] keyValue = str.split("=");
	    		if((changeOfStatus.keySet().contains(keyValue[0].replaceAll("\\s", ""))));
	    		     keyValue[1] = changeOfStatus.get(keyValue[0].replaceAll("\\s", ""));
	    		finalValues.put(keyValue[0], keyValue[1]);     
	    	}
	    }
	    values.remove(planId);
	    values.put(planId, finalValues.toString());
	    return values;
		
		
	}
	
	public static String getUpdateQuery(Map<String,String> values,String tableName,String keySpace,String primaryKey,String columnName,DataType colType,String PrimaryKeyName) {
		String formatedValue = MusicCore.convertToCQLDataType(colType, values);
		String query = "Update "+keySpace+"."+tableName+ " SET "+columnName+ " = "+ formatedValue +" WHERE "+ PrimaryKeyName+" = '"+ primaryKey+"';";
		
		return query;
		
	}
	
	public static  Object getColValue(Row row, String colName, DataType colType){	
		switch(colType.getName()){
		case VARCHAR: 
			return row.getString(colName);
		case UUID: 
			return row.getUUID(colName);
		case VARINT: 
			return row.getVarint(colName);
		case BIGINT: 
			return row.getLong(colName);
		case INT: 
			return row.getInt(colName);
		case FLOAT: 
			return row.getFloat(colName);	
		case DOUBLE: 
			return row.getDouble(colName);
		case BOOLEAN: 
			return row.getBool(colName);
		case MAP: 
			return row.getMap(colName, String.class, String.class);
		default: 
			return null;
		}
	}

}
