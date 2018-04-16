package com.att.research.music.conditionals;

import java.util.Map;

import com.att.research.music.main.ResultType;
import com.datastax.driver.core.ResultSet;

public class ResponseObject {
	private ResultType result;
	private String message;
	public ResultSet getUpdatedValues() {
		return updatedValues;
	}

	public void setUpdatedValues(ResultSet updatedValues) {
		this.updatedValues = updatedValues;
	}

	public void setResult(ResultType result) {
		this.result = result;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	private ResultSet updatedValues;
	public ResponseObject(ResultType result, String message) {
		super();
		this.result = result;
		this.message = message;
	}

	public ResponseObject(ResultType success, ResultSet updatedValues) {
		super();
		this.result=success;
		this.setUpdatedValues(updatedValues);

	}

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