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

package com.att.research.logging.format;

/**
 * @author inam
 *
 */
public enum AppMessages {
	
	
	
	/*
	 * 100-199 Security/Permission Related - Authentication problems
	 * [ERR100E] Missing Information 
	 * [ERR101E] Authentication error occured
	 * 
	 * 200-299 Availability/Timeout Related/IO - connectivity error - connection timeout
	 *  [ERR200E] Connectivity
	 *  [ERR201E] Host not available 
	 * 	[ERR202E] Error while connecting 
	 *  [ERR203E] IO Error has occured
	 *  [ERR204E] Execution Interrupted
	 * 	[ERR205E] Session Expired
	 *  
	 * 
	 * 
	 * 300-399 Data Access/Integrity Related
	 * [ERR300E] Incorrect data  
	 * 
	 * 400-499 - Cassandra Query Related
	 * 
	 * 
	 * 500-599 - Zookeepr/Locking Related
	 
	 * 
	 * 
	 * 600 - 699 - MDBC Service Errors
	 * [ERR600E] Error initializing the MDBC 
	 * 
	 * 700-799 Schema Interface Type/Validation - received Pay-load checksum is
	 * invalid - received JSON is not valid
	 * 
	 * 800-899 Business/Flow Processing Related - check out to service is not
	 * allowed - Roll-back is done - failed to generate heat file
	 * 
	 * 
	 * 900-999 Unknown Errors - Unexpected exception
	 * [ERR900E] Unexpected error occured
	 * [ERR901E] Number format exception  
	 * 
	 * 
	 * 1000-1099 Reserved - do not use
	 * 
	 */
	
	
	
	
	MISSINGINFO("[ERR100E]", "Missing Information ","Details: NA", "Please check application credentials and/or headers"),
	AUTHENTICATIONERROR("[ERR101E]", "Authentication error occured ","Details: NA", "Please verify application credentials"),
	
	CONNCECTIVITYERROR("[ERR200E]"," Connectivity error","Details: NA ","Please check connectivity to external resources"),
	HOSTUNAVAILABLE("[ERR201E]","Host not available","Details: NA","Please verify the host details"),
	IOERROR("[ERR203E]","IO Error has occured","","Please check IO"),
	EXECUTIONINTERRUPTED("[ERR204E]"," Execution Interrupted","",""),
	
	
	INCORRECTDATA("[ERR300E]"," Incorrect data",""," Please verify the request payload and try again"),
	MULTIPLERECORDS("[ERR301E]"," Multiple records found",""," Please verify the request payload and try again"),
	ALREADYEXIST("[ERR302E]"," Record already exist",""," Please verify the request payload and try again"),
	MISSINGDATA("[ERR300E]"," Incorrect data",""," Please verify the request payload and try again"),
	
	QUERYERROR("[ERR400E]","Error while processing query",""," Please verify the query"),
	
	
	UNKNOWNERROR("[ERR900E]"," Unexpected error occured",""," Please check logs for details");
	
	
		
	ErrorTypes eType;
	ErrorSeverity alarmSeverity;
	ErrorSeverity errorSeverity;
	String errorCode;
	String errorDescription;
	String details;
	String resolution;


	AppMessages(String errorCode, String errorDescription, String details,String resolution) {
	
		this.errorCode = errorCode;
		this.errorDescription = errorDescription;
		this.details = details;
		this.resolution = resolution;
	}

	
	
	
	AppMessages(ErrorTypes eType, ErrorSeverity alarmSeverity,
			ErrorSeverity errorSeverity, String errorCode, String errorDescription, String details,
			String resolution) {
	
		this.eType = eType;
		this.alarmSeverity = alarmSeverity;
		this.errorSeverity = errorSeverity;
		this.errorCode = errorCode;
		this.errorDescription = errorDescription;
		this.details = details;
		this.resolution = resolution;
	}

	public String getDetails() {
		return this.details;
	}

	public String getResolution() {
		return this.resolution;
	}

	public String getErrorCode() {
		return this.errorCode;
	}

	public String getErrorDescription() {
		return this.errorDescription;
	}

	

	
	
	

}
