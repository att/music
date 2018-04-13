package com.att.research.music.conditionals;

import java.io.Serializable;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonConditionalUpdate implements Serializable {
	
	private String primaryKey;
	private String primaryKeyValue;
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
	public String getCascadeColumnName() {
		return cascadeColumnName;
	}
	public void setCascadeColumnName(String cascadeColumnName) {
		this.cascadeColumnName = cascadeColumnName;
	}
	public Map<String, String> getUpdateStatus() {
		return updateStatus;
	}
	public void setUpdateStatus(Map<String, String> upateStatus) {
		this.updateStatus = upateStatus;
	}
	private String cascadeColumnName;
	private Map<String,String> updateStatus;
	
	
	

	

}
