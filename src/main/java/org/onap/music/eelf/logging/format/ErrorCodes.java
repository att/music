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
public enum ErrorCodes {
	
	
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
	
	/*SUCCESS("Success"), FAILURE("Failure");

    private String result;

    ResultType(String result) {
        this.result = result;
    }

    public String getResult() {
        return result;
    }
*/
	
	

}
