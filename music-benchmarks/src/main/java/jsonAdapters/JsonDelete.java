package jsonAdapters;

import java.util.ArrayList;
import java.util.Map;

public class JsonDelete {
	
    private ArrayList<String> columns = null;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

    public ArrayList<String> getColumns() {
		return columns;
	}
	public void setColumns(ArrayList<String> columns) {
		this.columns = columns;
	}
	String ttl, timestamp;

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
}
