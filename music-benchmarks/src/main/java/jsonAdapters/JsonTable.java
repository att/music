package jsonAdapters;
import java.util.Map;

public class JsonTable {
    private Map<String,String> fields;
	private Map<String, Object> properties; 
    private String clusteringOrder;
    private Map<String,String> consistencyInfo;


	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

    public Map<String, Object> getProperties() {
		return properties;
	}

	public void setProperties(Map<String, Object> properties) {
		this.properties = properties;
	}
    
	public Map<String, String> getFields() {
		return fields;
	}

	public void setFields(Map<String, String> fields) {
		this.fields = fields;
	}

	public String getClusteringOrder() {
		return clusteringOrder;
	}

	public void setClusteringOrder(String clusteringOrder) {
		this.clusteringOrder = clusteringOrder;
	}

}
