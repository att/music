package com.att.research.music.main;

import com.datastax.driver.core.ResultSet;

public class ReadReturnType {
	private ResultType result;
	private String message;
	ResultSet payload;
	public ReadReturnType(ResultType result, String message, ResultSet payload) {
		super();
		this.result = result;
		this.message = message;
		this.payload = payload; 
	}

	public ResultSet getPayload() {
		return payload;
	}
	public ResultType getResultType() {
		return result;
	}
	public String getTimingInfo() {
		return timingInfo;
	}

	public void setTimingInfo(String timingInfo) {
		this.timingInfo = timingInfo;
	}

	private String timingInfo;
	public ResultType getResult() {
		return result;
	}
	public String getMessage() {
		return message;
	}
	
	public String toString(){
		return result+" | "+message;
	}
}
