package com.att.research.music.conditionals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

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
			logger.error(e.getMessage());
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
				try {
					results = MusicCore.getDSHandle().executeCriticalGet(query.get("select"));
				} catch (Exception e) {
					logger.error(e.getMessage());
					return new ReturnType(ResultType.FAILURE, e.getMessage());
				}
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
			logger.error(e.getMessage());
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ReturnType(ResultType.FAILURE,
					"Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
							+ exceptionAsString);
		}

	}

	public static ResponseObject conditionalUpdate(Map<String, String> query, String tableName, String keySpace,
			String primaryKey, Map<String, String> changeOfStatus, String cascadeColumnName, String primaryKeyName,
			String planId) {

		long start = System.currentTimeMillis();
		String key = keySpace + "." + tableName + "." + primaryKey;
		String lockId = MusicCore.createLockReference(key);
		long leasePeriod = MusicUtil.defaultLockLeasePeriod;
		ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);
		try {
			if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				logger.info("acquired lock with id " + lockId);
				ResponseObject criticalPutResult = conditionalUpdateCriticalPut(keySpace, tableName, primaryKey, query,
						lockId, changeOfStatus, cascadeColumnName, primaryKeyName, planId);
				boolean voluntaryRelease = true;
				MusicCore.releaseLock(lockId, voluntaryRelease);
				long end = System.currentTimeMillis();
				logger.info("Time taken for the atomic put:" + (end - start) + " ms");
				return criticalPutResult;
			} else {
				logger.info("unable to acquire lock, id " + lockId);
				MusicCore.destroyLockRef(lockId);
				return new ResponseObject(ResultType.FAILURE, lockAcqResult.getMessage());
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			MusicCore.destroyLockRef(lockId);
			return new ResponseObject(ResultType.FAILURE, e.getMessage());
		}
	}

	public static ResponseObject conditionalUpdateCriticalPut(String keyspaceName, String tableName, String primaryKey,
			Map<String, String> query, String lockId, Map<String, String> changeOfStatus, String cascadeColumnName,
			String primaryKeyname, String planId) {
		// have an object that contains all the fields instead of sending way too many
		// params.
		ResultSet results = null;
		Row row = null;
		Map<String, String> updatedValues = new HashMap<String, String>();
		String updateQuery = "";
		try {
			MusicLockState mls = MusicCore.getLockingServiceHandle()
					.getLockState(keyspaceName + "." + tableName + "." + primaryKey);
			if (mls.getLockHolder().equals(lockId) == true) {
				try {
					results = MusicCore.getDSHandle().executeCriticalGet(query.get("select"));
				} catch (Exception e) {
					return new ResponseObject(ResultType.FAILURE, e.getMessage());
				}
				row = results.one();
				if (row != null) {
					if (planId != null || !planId.isEmpty() || planId.length() != 0)
						updatedValues = cascadeColumnUpdateSpecific(row, changeOfStatus, cascadeColumnName, planId);
					ColumnDefinitions colInfo = row.getColumnDefinitions();
					DataType colType = colInfo.getType(cascadeColumnName);
					updateQuery = getUpdateQuery(updatedValues, tableName, keyspaceName, primaryKey, cascadeColumnName,
							colType, primaryKeyname);
					try {
						MusicCore.getDSHandle().executePut(updateQuery, "critical");
						MusicCore.getDSHandle().executePut(query.get("upsert"), "critical");
						
					} catch (Exception e) {
						return new ResponseObject(ResultType.FAILURE, "update failed");
					}
				}
				else {
					return new ResponseObject(ResultType.FAILURE,"No data found");
				}
				results = MusicCore.getDSHandle().executeCriticalGet(query.get("select"));
				return new ResponseObject(ResultType.SUCCESS, results);
			} else
				return new ResponseObject(ResultType.FAILURE,
						"Cannot perform operation since you are the not the lock holder");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ResponseObject(ResultType.FAILURE,
					"Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
							+ exceptionAsString);
		}

	}

	@SuppressWarnings("unchecked")
	public static Map<String, String> cascadeColumnUpdateSpecific(Row row, Map<String, String> changeOfStatus,
			String cascadeColumnName, String planId) {

		ColumnDefinitions colInfo = row.getColumnDefinitions();
		DataType colType = colInfo.getType(cascadeColumnName);
		Map<String, String> values = new HashMap<>();
		Object columnValue = getColValue(row, cascadeColumnName, colType);

		Map<String, String> finalValues = new HashMap<>();
		values = (Map<String, String>) columnValue;
		if (values.keySet().contains(planId)) {
			String valueString = values.get(planId);
			String tempValueString = valueString.replaceAll("\\{", "").replaceAll("\"", "").replaceAll("\\}", "");
			String[] elements = tempValueString.split(",");
			for (String str : elements) {
				String[] keyValue = str.split(":");
				if ((changeOfStatus.keySet().contains(keyValue[0].replaceAll("\\s", ""))))
				keyValue[1] = changeOfStatus.get(keyValue[0].replaceAll("\\s", ""));
				finalValues.put(keyValue[0], keyValue[1]);
			}
		}
		values.remove(planId);
		JSONObject json = new JSONObject(finalValues);
		values.put(planId, json.toString());
		return values;

	}

	public static String getUpdateQuery(Map<String, String> values, String tableName, String keySpace,
			String primaryKey, String columnName, DataType colType, String PrimaryKeyName) {
		String formatedValue = MusicCore.convertToCQLDataType(colType, values);
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String query = "Update " + keySpace + "." + tableName + " SET " + columnName + " = " + formatedValue+ ", vector_ts = "+vectorTs + " WHERE "
				+ PrimaryKeyName + " = '" + primaryKey + "';";

		return query;

	}

	public static Object getColValue(Row row, String colName, DataType colType) {
		switch (colType.getName()) {
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
