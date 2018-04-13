package com.att.research.music.conditionals;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class JsonConditionalInsert implements Serializable {

	private String primaryKey;
	private String primaryKeyValue;
	private String cascadeColumnKey;
	private String cascadeColumnName;
	private Map<String, Object> nonExistsCondition;
	private Map<String, Object> existsCondition;
	private Map<String, Object> values;

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getPrimaryKeyValue() {
		return primaryKeyValue;
	}

	public void setPrimaryKeyValue(String primaryKeyValue) {
		this.primaryKeyValue = primaryKeyValue;
	}

	public String getCascadeColumnKey() {
		return cascadeColumnKey;
	}

	public void setCascadeColumnKey(String cascadeColumnKey) {
		this.cascadeColumnKey = cascadeColumnKey;
	}

	public String getCascadeColumnName() {
		return cascadeColumnName;
	}

	public void setCascadeColumnName(String cascadeColumnName) {
		this.cascadeColumnName = cascadeColumnName;
	}

	public Map<String, Object> getNonExistsCondition() {
		return nonExistsCondition;
	}

	public void setNonExistsCondition(Map<String, Object> nonExistsCondition) {
		this.nonExistsCondition = nonExistsCondition;
	}

	public Map<String, Object> getExistsCondition() {
		return existsCondition;
	}

	public void setExistsCondition(Map<String, Object> existsCondition) {
		this.existsCondition = existsCondition;
	}

	public Map<String, Object> getValues() {
		return values;
	}

	public void setValues(Map<String, Object> values) {
		this.values = values;
	}

}
