package jsonAdapters;

import java.util.Map;


public class JsonKeySpace {
    private Map<String,Object> replicationInfo;
	private String durabilityOfWrites;
    private Map<String,String> consistencyInfo;

	public Map<String, String> getConsistencyInfo() {
		return consistencyInfo;
	}

	public void setConsistencyInfo(Map<String, String> consistencyInfo) {
		this.consistencyInfo = consistencyInfo;
	}

	public Map<String, Object> getReplicationInfo() {
		return replicationInfo;
	}
	
	public void setReplicationInfo(Map<String, Object> replicationInfo) {
		this.replicationInfo = replicationInfo;
	}

	public String getDurabilityOfWrites() {
		return durabilityOfWrites;
	}
	public void setDurabilityOfWrites(String durabilityOfWrites) {
		this.durabilityOfWrites = durabilityOfWrites;
	}
		
	

}
