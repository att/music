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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONObject;

import com.att.research.music.main.MusicCore;
import com.att.research.music.main.ResultType;
import com.att.research.music.main.ReturnType;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;
import com.att.research.music.conditionals.*;

@Path("/conditional")
public class RestMusicConditonalAPI {
	final static Logger logger = Logger.getLogger(RestMusicConditonalAPI.class);

	@GET
	@Path("/version")
	@Produces(MediaType.TEXT_PLAIN)
	public String version() {
		return "MUSIC:";
	}

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
		Map<String, Object> nonExisitngValueMap = new HashMap<>();
		nonExisitngValueMap = insObj.getValues();
		Map<String, Object> exisitngValueMap = new HashMap<>();
		exisitngValueMap.putAll(nonExisitngValueMap);
		Map<String, Object> existingCondition = new HashMap<>();
		existingCondition = insObj.getExistsCondition();
		Map<String, Object> nonExistingCondition = new HashMap<>();
		nonExistingCondition = insObj.getNonExistsCondition();
		Map<String, String> tempValues = (Map<String, String>) existingCondition.get("value");
		Map<String, Object> tempValueMap = new HashMap<>();
		tempValueMap.put(cascadeColumnKey, tempValues.toString());
		Map<String, String> queries = new HashMap<>();
		//exisitngValueMap = formatValues(exisitngValueMap,existingCondition,cascadeColumnName, cascadeColumnKey, true);
		nonExisitngValueMap = formatValues(nonExisitngValueMap, nonExistingCondition, cascadeColumnName,
				cascadeColumnKey, true);
		 JSONObject json = null;
		 Map<String,Object> retunValue = new HashMap<>();
		try {
			tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		} catch (Exception ex) {
			logger.error(ex.getMessage());
			result = new ReturnType(ResultType.FAILURE, ex.getMessage());
			//return result;
		}
		String selectQuery = "SELECT * FROM " + keyspace + "." + tablename + " where " + primaryKey + " = '"
				+ primaryKeyValue + "';";
		DataType colType = tableInfo.getColumn(cascadeColumnName).getType();
		String formattedValue = MusicCore.convertToCQLDataType(colType, tempValueMap);
		String updateQuery = "UPDATE " + keyspace + "." + tablename + " SET " + cascadeColumnName + " = "
				+ cascadeColumnName + "+" + formattedValue.toString() + " WHERE " + primaryKey + " = '"
				+ primaryKeyValue + "';";
		String upsert = extractQuery(exisitngValueMap, tableInfo, tablename, keyspace);
		String insertQuery = extractQuery(nonExisitngValueMap, tableInfo, tablename, keyspace);
		queries.put("select", selectQuery);
		queries.put("update", updateQuery);
		queries.put("upsert", upsert);// need to simplify this
		queries.put("insert", insertQuery);
		try {
			result = MusicConditionalCore.conditionalInsert(queries, tablename, keyspace, primaryKeyValue);
			if(result.getMessage().equalsIgnoreCase("insert")) {
				retunValue.put("Status", "Success");
				retunValue.put("Operation", "Insert");
				retunValue.putAll(nonExisitngValueMap);
				return new JSONObject(retunValue);
			    
			}
			else
				retunValue.put("Status", "Success");
			    retunValue.put("Operation", "Update");
			    retunValue.put("Operation", "Appended " +cascadeColumnName+" with following values");
			    retunValue.put(cascadeColumnName+" ID", cascadeColumnKey);
			    retunValue.putAll(existingCondition);
			    return new JSONObject(retunValue);
			  
			 
		} catch (Exception e) {
			result = new ReturnType(ResultType.FAILURE, e.getMessage());
			retunValue.put("Status", "Failure");
			retunValue.put("Reason", e.getMessage());
			return new JSONObject(retunValue);
		    
			
		}
		
		
		

	}
	
	
	@SuppressWarnings("unchecked")
	@POST
	@Path("/update/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public JSONObject updateConditional(JsonConditionalUpdate upObj, @PathParam("keyspace") String keyspace,
			@PathParam("tablename") String tablename) {

		String primaryKey = upObj.getPrimaryKey();
		String primaryKeyValue = upObj.getPrimaryKeyValue();
		String cascadeColumnName = upObj.getCascadeColumnName();
		Map<String,String> changeOfStatus = upObj.getUpdateStatus();
		Map<String,String> queries = new HashMap<>();
		String selectQuery = "SELECT * FROM " + keyspace + "." + tablename + " where " + primaryKey + " = '"+ primaryKeyValue + "';";
		ReturnType result = null;
		Map<String,String> returnValue = new HashMap<>();
		queries.put("select",selectQuery );
		try {
		result =MusicConditionalCore.conditionalUpdate(queries, tablename, keyspace, primaryKeyValue, changeOfStatus, cascadeColumnName,primaryKey);
		String returnMsg = result.getMessage();
		returnValue.put("Status", "Success");
		returnValue.put("Operation", "Update");
		returnValue.put("Values", returnMsg);
		return new JSONObject(returnValue);
		}catch(Exception e) {
			returnValue.put("Status", "Failure");
			returnValue.put("Reason", e.getMessage());
			
		}
		String returnMsg = result.getMessage();
		
		return new JSONObject();

	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> formatValues(Map<String, Object> valueMap, Map<String, Object> conditions,
			String columnName, String columnValue, boolean isExists) {
		if (isExists) {
			Map<String, String> temp1 = (Map<String, String>) conditions.get("value");
			Map<String, String> temp2 = new HashMap<>();
			temp2.put(columnValue, temp1.toString());
			valueMap.put(columnName, temp2);
		}

		return valueMap;

	}

	public String extractQuery(Map<String, Object> valueMap, TableMetadata tableInfo, String tableName,
			String keySpaceName) {
		String fieldsString = "(";
		String valueString = "(";
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

}
