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

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;

@ApiModel(value = "JsonConditional", description = "Json model for insert or update into table based on some conditions")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonConditional implements Serializable {

	private String primaryKey;
	private String primaryKeyValue;
	private String casscadeColumnName;
	private Map<String,Object> tableValues;
	private Map<String,Object> casscadeColumnData;
	private Map<String,Map<String,String>> conditions;
	
	public Map<String, Object> getTableValues() {
		return tableValues;
	}
	public void setTableValues(Map<String, Object> tableValues) {
		this.tableValues = tableValues;
	}
	
	public String getPrimaryKey() {
		return primaryKey;
	}
	public String getPrimaryKeyValue() {
		return primaryKeyValue;
	}
	public String getCasscadeColumnName() {
		return casscadeColumnName;
	}

	public Map<String, Object> getCasscadeColumnData() {
		return casscadeColumnData;
	}

	
	
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public void setPrimaryKeyValue(String primaryKeyValue) {
		this.primaryKeyValue = primaryKeyValue;
	}
	public Map<String, Map<String, String>> getConditions() {
		return conditions;
	}
	public void setConditions(Map<String, Map<String, String>> conditions) {
		this.conditions = conditions;
	}
	public void setCasscadeColumnName(String casscadeColumnName) {
		this.casscadeColumnName = casscadeColumnName;
	}

	public void setCasscadeColumnData(Map<String, Object> casscadeColumnData) {
		this.casscadeColumnData = casscadeColumnData;
	}

	
	
	
	
}