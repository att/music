package com.att.research.music.main;
public class MusicDigest {
	private String evPutStatus;
	private String vectorTs;
	public MusicDigest(String evPutStatus, String vectorTs){
		this.evPutStatus = evPutStatus;
		this.vectorTs = vectorTs;
	}
	public String getEvPutStatus() {
		return evPutStatus;
	}
	public void setEvPutStatus(String evPutStatus) {
		this.evPutStatus = evPutStatus;
	}
	public String getVectorTs() {
		return vectorTs;
	}
	public void setVectorTs(String vectorTs) {
		this.vectorTs = vectorTs;
	}
	public String toString(){
		return vectorTs + "|" + evPutStatus;
	}
}

