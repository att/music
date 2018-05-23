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
package com.att.research.music.conditionals;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import com.att.research.music.main.MusicCore;
import com.att.research.music.main.ResultType;
import com.att.research.music.main.ReturnType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.att.research.music.conditionals.*;

@Path("/conditional")
public class RestMusicConditonalAPI {
	final static Logger logger = Logger.getLogger(RestMusicConditonalAPI.class);

	@SuppressWarnings("unchecked")
	@POST
	@Path("/insert/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public JSONObject insertConditional(JsonConditionalInsert insObj, @PathParam("keyspace") String keyspace,
			@PathParam("tablename") String tablename) {

		TableMetadata tableInfo = null;
		ReturnType result = null;
		String primaryKey = insObj.getPrimaryKey();
		String primaryKeyValue = insObj.getPrimaryKeyValue();
		String cascadeColumnName = insObj.getCascadeColumnName();
		String cascadeColumnKey = insObj.getCascadeColumnKey();
		Map<String, Object> valueMap = new HashMap<>();
		valueMap = insObj.getValues();
		Map<String, Object> valueMapClone = new HashMap<>();
		valueMapClone.putAll(valueMap);
		Map<String, Object> existingCondition = new HashMap<>();
		existingCondition = insObj.getExistsCondition();
		Map<String, Object> nonExistingCondition = new HashMap<>();
		nonExistingCondition = insObj.getNonExistsCondition();
		Map<String, String> existingConditionValues = (Map<String, String>) existingCondition.get("value");
		Map<String, String> nonExistingConditionValues = (Map<String, String>) nonExistingCondition.get("value");
		JSONObject existingConditionValuesJson = new JSONObject(existingConditionValues);
		JSONObject nonExistingConditionValuesJson = new JSONObject(nonExistingConditionValues);
		Map<String, Object> tempValueMap = new HashMap<>();
		tempValueMap.put(cascadeColumnKey, existingConditionValuesJson.toString());
		Map<String, String> queries = new HashMap<>();
		valueMap = formatValues(valueMap, nonExistingCondition, cascadeColumnName, cascadeColumnKey, true);
		JSONObject json = null;
		Map<String, Object> retunValue = new LinkedHashMap<>();
		try {
			tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			result = new ReturnType(ResultType.FAILURE, ex.getMessage());
			// return result;
		}
		String selectQuery = "SELECT * FROM " + keyspace + "." + tablename + " where " + primaryKey + " = '"
				+ primaryKeyValue + "';";
		DataType colType = tableInfo.getColumn(cascadeColumnName).getType();
		String formattedValue = MusicCore.convertToCQLDataType(colType, tempValueMap);
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String updateQuery = "UPDATE " + keyspace + "." + tablename + " SET " + cascadeColumnName + " = "
				+ cascadeColumnName + "+" + formattedValue.toString()+ " , vector_ts = "+vectorTs + " WHERE " + primaryKey + " = '"
				+ primaryKeyValue + "';";
		String upsert = extractQuery(valueMapClone, tableInfo, tablename, keyspace);
		String insertQuery = extractQuery(valueMap, tableInfo, tablename, keyspace);
		queries.put("select", selectQuery);
		queries.put("update", updateQuery);
		queries.put("upsert", upsert);// need to simplify this
		queries.put("insert", insertQuery);
		try {
			result = MusicConditionalCore.conditionalInsert(queries, tablename, keyspace, primaryKeyValue);
			if(result.getResult().equals(ResultType.FAILURE)) {
				retunValue.put("status", "failure");
				retunValue.put("reason", result.getMessage());
				return new JSONObject(retunValue);
			}
			retunValue.put("status", "success");
			if (result.getMessage().equalsIgnoreCase("insert")) {
				retunValue.put("operation", "insert");
				retunValue.put("row_values", parseValuesForOutPut(null, (Map<String, Object>) valueMapClone));
				retunValue.put("added_"+cascadeColumnName, cascadeColumnKey);
				retunValue.put(cascadeColumnName+"_values", nonExistingConditionValuesJson);
				return new JSONObject(retunValue);

			} else {
				retunValue.put("operation", "append");
				retunValue.put(primaryKey, primaryKeyValue);
				retunValue.put("added_"+cascadeColumnName, cascadeColumnKey);
				retunValue.put(cascadeColumnName+"_values", parseValuesForOutPut(existingConditionValues, null));
				return new JSONObject(retunValue);
			}

		} catch (Exception e) {
			result = new ReturnType(ResultType.FAILURE, e.getMessage());
			retunValue.put("status", "failure");
			retunValue.put("reason", e.getMessage());
			return new JSONObject(retunValue);

		}

	}

	@SuppressWarnings("unchecked")
	@PUT
	@Path("/update/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public JSONObject updateConditional(JsonConditionalUpdate upObj, @PathParam("keyspace") String keyspace,
			@PathParam("tablename") String tablename) {

		  
		String primaryKey = upObj.getPrimaryKey();
		String primaryKeyValue = upObj.getPrimaryKeyValue();
		String cascadeColumnName = upObj.getCascadeColumnName();
		String planId = upObj.getPlanId();
		Map<String, String> changeOfStatus = upObj.getUpdateStatus();
		Map<String, String> queries = new HashMap<>();
		TableMetadata tableInfo = null;
		Map<String, Object> returnValue = new LinkedHashMap<>();
		Map<String, Object> values = new HashMap<>();
		values = upObj.getValues();
		try {
			tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			returnValue.put("status", "failure");
			returnValue.put("reason", ex.getMessage());
			return new JSONObject(returnValue);
			// return result;
		}
		String selectQuery = "SELECT * FROM " + keyspace + "." + tablename + " where " + primaryKey + " = '"
				+ primaryKeyValue + "';";
		ResponseObject result = null;

		Map<String,Map<String,String>> rowValues = new LinkedHashMap<>();
		queries.put("select", selectQuery);
		String upsert = extractQuery(values, tableInfo, tablename, keyspace);
		queries.put("upsert", upsert);
		try {
			result = MusicConditionalCore.conditionalUpdate(queries, tablename, keyspace, primaryKeyValue,
					changeOfStatus, cascadeColumnName, primaryKey, planId);
		} catch (Exception e) {
			returnValue.put("status", "failure");
			returnValue.put("reason", e.getMessage());
			
		}
		if(result.getResult().equals(ResultType.FAILURE)) {
			returnValue.put("status", "failure");
			returnValue.put("reason", result.getMessage());
			return new JSONObject(returnValue);
			}
		
		/*for(Row row:result.getUpdatedValues()) {
			
			rowValues.putAll((Map<? extends String, ? extends Map<String, String>>) row.getObject(cascadeColumnName));
		}*///figure out marshalling data accordingly for output
        returnValue.put("status", "success");
		returnValue.put("operation", "update");
		returnValue.put(primaryKey, primaryKeyValue);
		returnValue.put("changed_"+cascadeColumnName, planId);
		returnValue.put(cascadeColumnName+"_values",parseValuesForOutPut(changeOfStatus, null));
		
		
		return new JSONObject(returnValue);

	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> formatValues(Map<String, Object> valueMap, Map<String, Object> conditions,
			String columnName, String columnValue, boolean isExists) {
		if (isExists) {
			Map<String, String> temp1 = (Map<String, String>) conditions.get("value");
			Map<String, String> temp2 = new HashMap<>();
			JSONObject json = new JSONObject(temp1);
			temp2.put(columnValue, json.toString());
			valueMap.put(columnName, temp2);
		}

		return valueMap;

	}

	public String extractQuery(Map<String, Object> valueMap, TableMetadata tableInfo, String tableName,
			String keySpaceName) {
		//String fieldsString = "(";
		//String valueString = "(";
		String fieldsString="(vector_ts,";
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String valueString ="("+vectorTs+",";
		int counter = 0;
		for (Map.Entry<String, Object> entry : valueMap.entrySet()) {
			fieldsString = fieldsString + "" + entry.getKey();
			Object valueObj = entry.getValue();

			DataType colTypes = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValues = MusicCore.convertToCQLDataType(colTypes, valueObj);
			valueString = valueString + formattedValues;
			if (counter == valueMap.size() - 1) {
				fieldsString = fieldsString + ")";
				valueString = valueString + ")";
			} else {
				fieldsString = fieldsString + ",";
				valueString = valueString + ",";
			}
			counter = counter + 1;
		}
		String insertQuery = "INSERT INTO " + keySpaceName + "." + tableName + " " + fieldsString + " VALUES "
				+ valueString + ";";
		return insertQuery;

	}

	public JSONObject parseValuesForOutPut(Map<String, String> stringMap, Map<String, Object> objectMap) {
		JSONObject json = null;
		if (stringMap == null)
			json = new JSONObject(objectMap);
		if (objectMap == null)
			json = new JSONObject(stringMap);
		return json;
	

	}

}
