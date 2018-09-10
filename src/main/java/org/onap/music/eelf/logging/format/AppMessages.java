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

package org.onap.music.eelf.logging.format;

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
	 * 	[ERR202E] Error while connecting to Cassandra cluster
	 *  [ERR203E] IO Error has occured
	 *  [ERR204E] Execution Interrupted
	 * 	[ERR205E] Session Expired
	 *  [ERR206E] Cache not authenticated
	 * 
	 * 
	 * 300-399 Data Access/Integrity Related
	 * [ERR300E] Incorrect data  
	 * 
	 * 400-499 - Cassandra Query Related
	 * [ERR400E] Error while processing prepared query object
	 * [ERR401E] Executing Session Failure for Request
	 * [ERR402E] Ill formed queryObject for the request
	 * [ERR403E] Error processing Prepared Query Object  
	 * 
	 * 500-599 - Zookeepr/Locking Related
	 * [ERR500E] Invalid lock
	 * [ERR501E] Locking Error has occured
	 * [ERR502E] Zookeeper error has occured
	 * [ERR503E] Failed to aquire lock store handle  
	 * [ERR504E] Failed to create Lock Reference
	 * [ERR505E] Lock does not exist
	 * [ERR506E] Failed to aquire lock
	 * [ERR507E] Lock not aquired 
	 * [ERR508E] Lock state not set 
	 * [ERR509E] Lock not destroyed 
	 * [ERR510E] Lock not released 
	 * [ERR511E] Lock not deleted 
	 * [ERR512E] Failed to get ZK Lock Handle
	 * 
	 * 
	 * 600 - 699 - Music Service Errors
	 * [ERR600E] Error initializing the cache 
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
	CASSANDRACONNECTIVITY("[ERR202E]","Error while connecting to Cassandra cluster",""," Please check cassandra cluster details"),
	IOERROR("[ERR203E]","IO Error has occured","","Please check IO"),
	EXECUTIONINTERRUPTED("[ERR204E]"," Execution Interrupted","",""),
	SESSIONEXPIRED("[ERR205E]"," Session Expired","","Session has expired."),
	CACHEAUTHENTICATION("[ERR206E]","Cache not authenticated",""," Cache not authenticated"),
	
	INCORRECTDATA("[ERR300E]"," Incorrect data",""," Please verify the request payload and try again"),
	MULTIPLERECORDS("[ERR301E]"," Multiple records found",""," Please verify the request payload and try again"),
	ALREADYEXIST("[ERR302E]"," Record already exist",""," Please verify the request payload and try again"),
	MISSINGDATA("[ERR300E]"," Incorrect data",""," Please verify the request payload and try again"),
	
	QUERYERROR("[ERR400E]","Error while processing prepared query object",""," Please verify the query"),
	SESSIONFAILED("[ERR401E]","Executing Session Failure for Request","","Please verify the session and request"),
	
	INVALIDLOCK("[ERR500E]"," Invalid lock or acquire failed",""," Lock is not valid to aquire"),
	LOCKINGERROR("[ERR501E]"," Locking Error has occured",""," Locking Error has occured"),
	KEEPERERROR("[ERR502E]"," Zookeeper error has occured","","Please check zookeeper details"),
	LOCKHANDLE("[ERR503E]","Failed to aquire lock store handle",""," Failed to aquire lock store handle"),
	CREATELOCK("[ERR504E]","Failed to aquire lock store handle  ","","Failed to aquire lock store handle  "),
	LOCKSTATE("[ERR508E]"," Lock state not set",""," Lock state not set"),
	DESTROYLOCK("[ERR509E]"," Lock not destroyed",""," Lock not destroyed"),
	RELEASELOCK("[ERR510E]"," Lock not released",""," Lock not released"),
	DELTELOCK("[ERR511E]",""," Lock not deleted "," Lock not deleted "),
	CACHEERROR("[ERR600E]"," Error initializing the cache",""," Error initializing the cache"),
	
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
