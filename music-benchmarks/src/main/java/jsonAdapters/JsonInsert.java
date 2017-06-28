package jsonAdapters;

import java.util.Map;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
public class JsonInsert {
	
    private Map<String,Object> values;
    String ttl, timestamp;
	private Map<String,Object> row_specification;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

	public String getTtl() {
		return ttl;
	}
	public void setTtl(String ttl) {
		this.ttl = ttl;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public Map<String, Object> getValues() {
		return values;
	}
	public void setValues(Map<String, Object> values) {
		this.values = values;
	}
    public Map<String, Object> getRow_specification() {
		return row_specification;
	}
	public void setRow_specification(Map<String, Object> row_specification) {
		this.row_specification = row_specification;
	}
}
