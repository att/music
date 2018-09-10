/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.conductor.conditionals;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jettison.json.JSONObject;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.rest.RestMusicDataAPI;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

public class MusicConditional {
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);

	public static ReturnType conditionalInsert(String keyspace, String tablename, String casscadeColumnName,
			Map<String, Object> casscadeColumnData, String primaryKey, Map<String, Object> valuesMap,
			Map<String, String> status) throws Exception {

		Map<String, PreparedQueryObject> queryBank = new HashMap<>();
		TableMetadata tableInfo = null;
		tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		DataType primaryIdType = tableInfo.getPrimaryKey().get(0).getType();
		String primaryId = tableInfo.getPrimaryKey().get(0).getName();
		DataType casscadeColumnType = tableInfo.getColumn(casscadeColumnName).getType();
		String vector = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());

		PreparedQueryObject select = new PreparedQueryObject();
		select.appendQueryString("SELECT * FROM " + keyspace + "." + tablename + " where " + primaryId + " = ?");
		select.addValue(MusicUtil.convertToActualDataType(primaryIdType, primaryKey));
		queryBank.put(MusicUtil.SELECT, select);

		PreparedQueryObject update = new PreparedQueryObject();
		Map<String, String> updateColumnvalues = new HashMap<>(); //casscade column values
		updateColumnvalues = getValues(true, casscadeColumnData, status);
		Object formatedValues = MusicUtil.convertToActualDataType(casscadeColumnType, updateColumnvalues);
		update.appendQueryString("UPDATE " + keyspace + "." + tablename + " SET " + casscadeColumnName + " ="
				+ casscadeColumnName + " + ? , vector_ts = ?" + " WHERE " + primaryId + " = ? ");
		update.addValue(formatedValues);
		update.addValue(MusicUtil.convertToActualDataType(DataType.text(), vector));
		update.addValue(MusicUtil.convertToActualDataType(primaryIdType, primaryKey));
		queryBank.put(MusicUtil.UPDATE, update);


		Map<String, String> insertColumnvalues = new HashMap<>();//casscade column values
		insertColumnvalues = getValues(false, casscadeColumnData, status);
		formatedValues = MusicUtil.convertToActualDataType(casscadeColumnType, insertColumnvalues);
		PreparedQueryObject insert = extractQuery(valuesMap, tableInfo, tablename, keyspace, primaryId, primaryKey,casscadeColumnName,formatedValues);
		queryBank.put(MusicUtil.INSERT, insert);
		
		
		String key = keyspace + "." + tablename + "." + primaryKey;
		String lockId = MusicCore.createLockReference(key);
		long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
		ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);

		try {
			if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				ReturnType criticalPutResult = conditionalInsertAtomic(lockId, keyspace, tablename, primaryKey,
						queryBank);
				MusicCore.destroyLockRef(key,lockId);
				if (criticalPutResult.getMessage().contains("insert"))
					criticalPutResult
							.setMessage("Insert values: ");
				else if (criticalPutResult.getMessage().contains("update"))
					criticalPutResult
							.setMessage("Update values: " + updateColumnvalues);
				return criticalPutResult;

			} else {
				MusicCore.destroyLockRef(key,lockId);
				return lockAcqResult;
			}
		} catch (Exception e) {
			MusicCore.destroyLockRef(key,lockId);
			return new ReturnType(ResultType.FAILURE, e.getMessage());
		}

	}

	public static ReturnType conditionalInsertAtomic(String lockId, String keyspace, String tableName,
			String primaryKey, Map<String, PreparedQueryObject> queryBank) {

		ResultSet results = null;

		try {
			String fullyQualifiedKey = keyspace + "." + tableName + "." + primaryKey;
	        ReturnType lockAcqResult = MusicCore.acquireLock(fullyQualifiedKey, lockId);
	        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				try {
					results = MusicCore.getDSHandle().executeCriticalGet(queryBank.get(MusicUtil.SELECT));
				} catch (Exception e) {
					return new ReturnType(ResultType.FAILURE, e.getMessage());
				}
				if (results.all().isEmpty()) {
					MusicCore.getDSHandle().executePut(queryBank.get(MusicUtil.INSERT), "critical");
					return new ReturnType(ResultType.SUCCESS, "insert");
				} else {
					MusicCore.getDSHandle().executePut(queryBank.get(MusicUtil.UPDATE), "critical");
					return new ReturnType(ResultType.SUCCESS, "update");
				}
			} else {
				return new ReturnType(ResultType.FAILURE,
						"Cannot perform operation since you are the not the lock holder");
			}

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ReturnType(ResultType.FAILURE,
					"Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
							+ exceptionAsString);
		}

	}

	public static ReturnType update(Map<String,PreparedQueryObject> queryBank, String keyspace, String tableName, String primaryKey,String primaryKeyValue,String planId,String cascadeColumnName,Map<String,String> cascadeColumnValues) {

		String key = keyspace + "." + tableName + "." + primaryKeyValue;
		String lockId = MusicCore.createLockReference(key);
		long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
		try {
		ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);
			if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				return updateAtomic(lockId, keyspace, tableName, primaryKey,primaryKeyValue, queryBank,planId,cascadeColumnValues,cascadeColumnName);

			} else {
				MusicCore.destroyLockRef(key,lockId);
				return lockAcqResult;
			}

		} catch (Exception e) {
			MusicCore.destroyLockRef(key,lockId);
			return new ReturnType(ResultType.FAILURE, e.getMessage());

		}
	}

	public static ReturnType updateAtomic(String lockId, String keyspace, String tableName, String primaryKey,String primaryKeyValue,
			Map<String,PreparedQueryObject> queryBank,String planId,Map<String,String> cascadeColumnValues,String casscadeColumnName) {
		String key = keyspace + "." + tableName + "." + primaryKeyValue;
		long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
		try {
			ReturnType lockAcqResult = MusicCore.acquireLockWithLease(key, lockId, leasePeriod);
			if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
				Row row  = MusicCore.getDSHandle().executeCriticalGet(queryBank.get(MusicUtil.SELECT)).one();
				
				if(row != null) {
					Map<String, String> updatedValues = cascadeColumnUpdateSpecific(row, cascadeColumnValues, casscadeColumnName, planId);
					JSONObject json = new JSONObject(updatedValues);
					PreparedQueryObject update = new PreparedQueryObject();
					String vector_ts = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
					update.appendQueryString("UPDATE " + keyspace + "." + tableName + " SET " + casscadeColumnName + "['" + planId
							+ "'] = ?, vector_ts = ? WHERE " + primaryKey + " = ?");
					update.addValue(MusicUtil.convertToActualDataType(DataType.text(), json.toString()));
					update.addValue(MusicUtil.convertToActualDataType(DataType.text(), vector_ts));
					update.addValue(MusicUtil.convertToActualDataType(DataType.text(), primaryKeyValue));
					try {
						MusicCore.getDSHandle().executePut(update, "critical");
					} catch (Exception ex) {
						return new ReturnType(ResultType.FAILURE, ex.getMessage());
					}
				}else {
					return new ReturnType(ResultType.FAILURE,"Cannot find data related to key: "+primaryKey);
				}
				MusicCore.getDSHandle().executePut(queryBank.get(MusicUtil.UPSERT), "critical");
				return new ReturnType(ResultType.SUCCESS, "update success");

			} else {
				return new ReturnType(ResultType.FAILURE,
						"Cannot perform operation since you are the not the lock holder");
			}

		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			return new ReturnType(ResultType.FAILURE,
					"Exception thrown while doing the critical put, check sanctity of the row/conditions:\n"
							+ exceptionAsString);
		}

	}

	@SuppressWarnings("unchecked")
	public static Map<String, String> getValues(boolean isExists, Map<String, Object> casscadeColumnData,
			Map<String, String> status) {

		Map<String, String> value = new HashMap<>();
		Map<String, String> returnMap = new HashMap<>();
		Object key = casscadeColumnData.get("key");
		String setStatus = "";
		value = (Map<String, String>) casscadeColumnData.get("value");

		if (isExists)
			setStatus = status.get("exists");
		else
			setStatus = status.get("nonexists");

		value.put("status", setStatus);
		JSONObject valueJson = new JSONObject(value);
		returnMap.put(key.toString(), valueJson.toString());
		return returnMap;

	}
	
	public static PreparedQueryObject extractQuery(Map<String, Object> valuesMap, TableMetadata tableInfo, String tableName,
			String keySpaceName,String primaryKeyName,String primaryKey,String casscadeColumn,Object casscadeColumnValues) throws Exception {

		PreparedQueryObject queryObject = new PreparedQueryObject();
		StringBuilder fieldsString = new StringBuilder("(vector_ts"+",");
		StringBuilder valueString = new StringBuilder("(" + "?" + ",");
		String vector = String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
		queryObject.addValue(vector);
		if(casscadeColumn!=null && casscadeColumnValues!=null) {
			fieldsString.append("" +casscadeColumn+" ," );
		  valueString.append("?,");
		  queryObject.addValue(casscadeColumnValues);
		}
		
		int counter = 0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            
			fieldsString.append("" + entry.getKey());
            Object valueObj = entry.getValue();
            if (primaryKeyName.equals(entry.getKey())) {
                primaryKey = entry.getValue() + "";
                primaryKey = primaryKey.replace("'", "''");
            }
            DataType colType = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
            } catch(NullPointerException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() +" Invalid column name : "+entry.getKey(), AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
               
            }

            Object formattedValue = null;
            try {
              formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
          }
            
			valueString.append("?");
            queryObject.addValue(formattedValue);

            
			if (counter == valuesMap.size() - 1) {
                fieldsString.append(")");
                valueString.append(")");
            } else {
                fieldsString.append(",");
                valueString.append(",");
            }
            counter = counter + 1;
        }
        queryObject.appendQueryString("INSERT INTO " + keySpaceName + "." + tableName + " "
                + fieldsString + " VALUES " + valueString);
		return queryObject;
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
		return finalValues;

	}

}