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
package org.onap.music.main;


import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import org.onap.music.datastore.CassaDataStore;
import org.onap.music.datastore.CassaLockStore;
import org.onap.music.datastore.CassaLockStore.LockObject;
import org.onap.music.datastore.MusicLockState;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;


/**
 * This class .....
 * 
 *
 */
public class MusicCore {

    public static CassaLockStore mLockHandle = null;
    public static CassaDataStore mDstoreHandle = null;
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicCore.class);
    private static boolean unitTestRun=true;
    
    public static class Condition {
        Map<String, Object> conditions;
        PreparedQueryObject selectQueryForTheRow;

        public Condition(Map<String, Object> conditions, PreparedQueryObject selectQueryForTheRow) {
            this.conditions = conditions;
            this.selectQueryForTheRow = selectQueryForTheRow;
        }

        public boolean testCondition() throws Exception {
            // first generate the row
            ResultSet results = quorumGet(selectQueryForTheRow);
            Row row = results.one();
            return getDSHandle().doesRowSatisfyCondition(row, conditions);
        }
    }


    public static CassaLockStore getLockingServiceHandle() throws MusicLockingException {
        logger.info(EELFLoggerDelegate.applicationLogger,"Acquiring lock store handle");
        long start = System.currentTimeMillis();

        if (mLockHandle == null) {
            try {
                mLockHandle = new CassaLockStore(getDSHandle());
            } catch (Exception e) {
            	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.LOCKHANDLE,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
                throw new MusicLockingException("Failed to aquire Locl store handle " + e);
            }
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to acquire lock store handle:" + (end - start) + " ms");
        return mLockHandle;
    }

    /**
     * 
     * @param remoteIp
     * @return
     */
    public static CassaDataStore getDSHandle(String remoteIp) {
        logger.info(EELFLoggerDelegate.applicationLogger,"Acquiring data store handle");
        long start = System.currentTimeMillis();
        if (mDstoreHandle == null) {
            mDstoreHandle = new CassaDataStore(remoteIp);
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to acquire data store handle:" + (end - start) + " ms");
        return mDstoreHandle;
    }

    /**
     * 
     * @return
     * @throws MusicServiceException 
     */
    public static CassaDataStore getDSHandle() throws MusicServiceException {
		
        logger.info(EELFLoggerDelegate.applicationLogger,"Acquiring data store handle");
        long start = System.currentTimeMillis();
        if (mDstoreHandle == null) {
            // Quick Fix - Best to put this into every call to getDSHandle?
            if (! MusicUtil.getMyCassaHost().equals("localhost") ) {
                mDstoreHandle = new CassaDataStore(MusicUtil.getMyCassaHost());
            } else {
                mDstoreHandle = new CassaDataStore();
            }
        }
        if(mDstoreHandle.getSession() == null) {
        	String message = "Connection to Cassandra has not been enstablished."
        			+ " Please check connection properites and reboot.";
        	logger.info(EELFLoggerDelegate.applicationLogger, message);
            throw new MusicServiceException(message);
        }
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to acquire data store handle:" + (end - start) + " ms");
        return mDstoreHandle;
    }

    public static String createLockReference(String fullyQualifiedKey) {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String lockName = splitString[2];

        logger.info(EELFLoggerDelegate.applicationLogger,"Creating lock reference for lock name:" + lockName);
        long start = System.currentTimeMillis();
        String lockReference = null;
        try {
			lockReference = "" + getLockingServiceHandle().genLockRefandEnQueue(keyspace, table, lockName);
		} catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
			e.printStackTrace();
		}
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to create lock reference:" + (end - start) + " ms");
        return lockReference;
    }


    public static ReturnType acquireLockWithLease(String fullyQualifiedKey, String lockReference, long leasePeriod) throws MusicLockingException, MusicQueryException, MusicServiceException  {
     	evictExpiredLockHolder(fullyQualifiedKey,leasePeriod);
    		return acquireLock(fullyQualifiedKey, lockReference);
    }

    private static void evictExpiredLockHolder(String fullyQualifiedKey, long leasePeriod) throws MusicLockingException, MusicQueryException, MusicServiceException {

        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];

		LockObject currentLockHolderObject = getLockingServiceHandle().peekLockQueue(keyspace, table, primaryKeyValue);
		
		/* Release the lock of the previous holder if it has expired. if the update to the acquire time has not reached due to network delays, simply use the create time as the 
		 * reference*/
		
		long referenceTime = Math.max(Long.parseLong(currentLockHolderObject.acquireTime), Long.parseLong(currentLockHolderObject.createTime));
		if((System.currentTimeMillis() - referenceTime) > leasePeriod) {
			forciblyReleaseLock(fullyQualifiedKey,  currentLockHolderObject.lockRef+"");
            logger.info(EELFLoggerDelegate.applicationLogger, currentLockHolderObject.lockRef+" forcibly released");
		}    	
    }
    
    private static ReturnType isTopOfLockStore(String keyspace, String table, String primaryKeyValue, String lockReference) throws MusicLockingException, MusicQueryException, MusicServiceException {
        
        //return failure to lock holders too early or already evicted from the lock store
        String topOfLockStoreS = getLockingServiceHandle().peekLockQueue(keyspace, table, primaryKeyValue).lockRef;
        long topOfLockStoreL = Long.parseLong(topOfLockStoreS);
        long lockReferenceL = Long.parseLong(lockReference);

        if(lockReferenceL > topOfLockStoreL) {
            logger.info(EELFLoggerDelegate.applicationLogger, lockReference+" is not the lock holder yet");
            return new ReturnType(ResultType.FAILURE, lockReference+" is not the lock holder yet");
        }
    			

        if(lockReferenceL < topOfLockStoreL) {
            logger.info(EELFLoggerDelegate.applicationLogger, lockReference+" is no longer/or was never in the lock store queue");
            return new ReturnType(ResultType.FAILURE, lockReference+" is no longer/or was never in the lock store queue");
       	}
    		
       	return new ReturnType(ResultType.SUCCESS, lockReference+" is top of lock store");
    }
    
    public static ReturnType acquireLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicQueryException, MusicServiceException {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];

        ReturnType result = isTopOfLockStore(keyspace, table, primaryKeyValue, lockReference);
        
        if(result.getResult().equals(ResultType.FAILURE))
        		return result;//not top of the lock store q
    		
        //check to see if the value of the key has to be synced in case there was a forceful release
        String syncTable = keyspace+".unsyncedKeys_"+table;
		String query = "select * from "+syncTable+" where key='"+fullyQualifiedKey+"';";
        PreparedQueryObject readQueryObject = new PreparedQueryObject();
        readQueryObject.appendQueryString(query);
		ResultSet results = getDSHandle().executeCriticalGet(readQueryObject);			
		if (results.all().size() != 0) {
			logger.info("In acquire lock: Since there was a forcible release, need to sync quorum!");
			try {
				syncQuorum(keyspace, table, primaryKeyValue);
			} catch (Exception e) {
				StringWriter sw = new StringWriter();
	           	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), "[ERR506E] Failed to aquire lock ",ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
	            String exceptionAsString = sw.toString();
	            return new ReturnType(ResultType.FAILURE, "Exception thrown while syncing key:\n" + exceptionAsString);			
	        }
			String cleanQuery = "delete * from music_internal.unsynced_keys where key='"+fullyQualifiedKey+"';";
	        PreparedQueryObject deleteQueryObject = new PreparedQueryObject();
	        deleteQueryObject.appendQueryString(cleanQuery);
			getDSHandle().executePut(deleteQueryObject, "critical");
		}
		
		getLockingServiceHandle().updateLockAcquireTime(keyspace, table, primaryKeyValue, lockReference);
		
		return new ReturnType(ResultType.SUCCESS, lockReference+" is the lock holder for the key");
    }



    /**
     * 
     * @param tableQueryObject
     * @param consistency
     * @return Boolean Indicates success or failure
     * @throws MusicServiceException 
     * 
     * 
     */
    public static ResultType createTable(String keyspace, String table, PreparedQueryObject tableQueryObject, String consistency) throws MusicServiceException {
	    	boolean result = false;
	
	    	try {
		    	//create shadow locking table 
	    		result = getLockingServiceHandle().createLockQueue(keyspace, table);
	    		if(result == false) 
	    			return ResultType.FAILURE;
	
	    		result = false;
	    		
	    		//create table to track unsynced_keys
	    		table = "unsyncedKeys_"+table; 
	    		
	    		String tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table
	    				+ " ( key text,PRIMARY KEY (key) );";
	    		System.out.println(tabQuery);
	    		PreparedQueryObject queryObject = new PreparedQueryObject(); 
	    		
	    		queryObject.appendQueryString(tabQuery);
	    		result = false;
	    		result = getDSHandle().executePut(queryObject, "eventual");

	    	
	    		//create actual table
	    		result = getDSHandle().executePut(tableQueryObject, consistency);
	    	} catch (MusicQueryException | MusicServiceException | MusicLockingException ex) {
	    		logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
	    		throw new MusicServiceException(ex.getMessage());
	    	}
	    	return result?ResultType.SUCCESS:ResultType.FAILURE;
    }

    private static void syncQuorum(String keyspace, String table, String primaryKeyValue) throws Exception {
        logger.info(EELFLoggerDelegate.applicationLogger,"Performing sync operation---");
        PreparedQueryObject selectQuery = new PreparedQueryObject();
        PreparedQueryObject updateQuery = new PreparedQueryObject();

        // get the primary key d
        TableMetadata tableInfo = returnColumnMetadata(keyspace, table);
        String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();// we only support single
                                                                           // primary key
        DataType primaryKeyType = tableInfo.getPrimaryKey().get(0).getType();
        Object cqlFormattedPrimaryKeyValue =
                        MusicUtil.convertToActualDataType(primaryKeyType, primaryKeyValue);

        // get the row of data from a quorum
        selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + table + " WHERE "
                        + primaryKeyName + "= ?" + ";");
        selectQuery.addValue(cqlFormattedPrimaryKeyValue);
        ResultSet results = null;
        try {
            results = getDSHandle().executeCriticalGet(selectQuery);
            // write it back to a quorum
            Row row = results.one();
            ColumnDefinitions colInfo = row.getColumnDefinitions();
            int totalColumns = colInfo.size();
            int counter = 1;
            StringBuilder fieldValueString = new StringBuilder("");
            for (Definition definition : colInfo) {
                String colName = definition.getName();
                if (colName.equals(primaryKeyName))
                    continue;
                DataType colType = definition.getType();
                Object valueObj = getDSHandle().getColValue(row, colName, colType);
                Object valueString = MusicUtil.convertToActualDataType(colType, valueObj);
                fieldValueString.append(colName + " = ?");
                updateQuery.addValue(valueString);
                if (counter != (totalColumns - 1))
                    fieldValueString.append(",");
                counter = counter + 1;
            }
            updateQuery.appendQueryString("UPDATE " + keyspace + "." + table + " SET "
                            + fieldValueString + " WHERE " + primaryKeyName + "= ? " + ";");
            updateQuery.addValue(cqlFormattedPrimaryKeyValue);

            getDSHandle().executePut(updateQuery, "critical");
        } catch (MusicServiceException | MusicQueryException e) {
        	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.QUERYERROR +""+updateQuery ,ErrorSeverity.MAJOR, ErrorTypes.QUERYERROR);
        }
    }




    /**
     * 
     * @param query
     * @return ResultSet
     */
    public static ResultSet quorumGet(PreparedQueryObject query) {
        ResultSet results = null;
        try {
            results = getDSHandle().executeCriticalGet(query);
        } catch (MusicServiceException | MusicQueryException e) {
        	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR ,ErrorSeverity.MAJOR, ErrorTypes.GENERALSERVICEERROR);
        
        }
        return results;

    }

    /**
     * 
     * @param results
     * @return
     * @throws MusicServiceException 
     */
    public static Map<String, HashMap<String, Object>> marshallResults(ResultSet results) throws MusicServiceException {
        return getDSHandle().marshalData(results);
    }

    /**
     * 
     * @param fullyQualifiedKey lockName
     * @return
     */
    public static String whoseTurnIsIt(String fullyQualifiedKey) {
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];
        try {
            return getLockingServiceHandle().peekLockQueue(keyspace, table, primaryKeyValue)+"";
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
         	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.LOCKINGERROR+fullyQualifiedKey ,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
        }
        return null;
    }

    /**
     * 
     * @param lockReference
     * @return
     */
    public static String getLockNameFromId(String lockReference) {
        StringTokenizer st = new StringTokenizer(lockReference);
        return st.nextToken("$");
    }

    public static MusicLockState destroyLockRef(String fullyQualifiedKey, String lockReference) {
        long start = System.currentTimeMillis();
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];
        String primaryKeyValue = splitString[2];
        try {
            getLockingServiceHandle().deQueueLockRef(keyspace, table, primaryKeyValue, lockReference);
        } catch (MusicLockingException | MusicServiceException | MusicQueryException e) {
        	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.DESTROYLOCK+lockReference  ,ErrorSeverity.CRITICAL, ErrorTypes.LOCKINGERROR);
        } 
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,"Time taken to destroy lock reference:" + (end - start) + " ms");
        return getMusicLockState(fullyQualifiedKey);
    }

    public static  MusicLockState  voluntaryReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException{
		return destroyLockRef(fullyQualifiedKey, lockReference);
	}

    public static  MusicLockState  forciblyReleaseLock(String fullyQualifiedKey, String lockReference) throws MusicLockingException, MusicServiceException, MusicQueryException{
        String[] splitString = fullyQualifiedKey.split("\\.");
        String keyspace = splitString[0];
        String table = splitString[1];

    		//leave a signal that this key could potentially be unsynchronized
        String syncTable = keyspace+".unsyncedKeys_"+table;	
        PreparedQueryObject queryObject = new PreparedQueryObject();
		String values = "(?)";
		queryObject.addValue(fullyQualifiedKey);
		String insQuery = "insert into "+syncTable+" (key) values "+values+"';";
        queryObject.appendQueryString(insQuery);
        getDSHandle().executePut(queryObject, "critical");	
        
        //now release the lock
		return destroyLockRef(fullyQualifiedKey, lockReference);
	}

    /**
     * 
     * @param lockName
     * @throws MusicLockingException 
     */
    public static void deleteLock(String lockName) throws MusicLockingException {
    		//deprecated
    	}



    /**
     * 
     * @param keyspace
     * @param tablename
     * @return
     * @throws MusicServiceException 
     */
    public static TableMetadata returnColumnMetadata(String keyspace, String tablename) throws MusicServiceException {
        return getDSHandle().returnColumnMetadata(keyspace, tablename);
    }




    // Prepared Query Additions.

    /**
     * 
     * @param queryObject
     * @return ReturnType
     * @throws MusicServiceException
     */
    public static ReturnType eventualPut(PreparedQueryObject queryObject) {
        boolean result = false;
        try {
            result = getDSHandle().executePut(queryObject, MusicUtil.EVENTUAL);
        } catch (MusicServiceException | MusicQueryException ex) {
        	logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), "[ERR512E] Failed to get ZK Lock Handle "  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() + "  " + ex.getCause() + " " + ex);
            return new ReturnType(ResultType.FAILURE, ex.getMessage());
        }
        if (result) {
            return new ReturnType(ResultType.SUCCESS, "Success");
        } else {
            return new ReturnType(ResultType.FAILURE, "Failure");
        }
    }
    
    /**
     * 
     * @param keyspace
     * @param table
     * @param primaryKeyValue
     * @param queryObject
     * @param lockReference
     * @return
     */
    public static ReturnType criticalPut(String keyspace, String table, String primaryKeyValue,
                    PreparedQueryObject queryObject, String lockReference, Condition conditionInfo) {
        long start = System.currentTimeMillis();
        try {
        ReturnType result = isTopOfLockStore(keyspace, table, primaryKeyValue, lockReference);
        if(result.getResult().equals(ResultType.FAILURE))
        		return result;//not top of the lock store q

        if (conditionInfo != null)
            try {
              if (conditionInfo.testCondition() == false)
                  return new ReturnType(ResultType.FAILURE,
                                  "Lock acquired but the condition is not true");
            } catch (Exception e) {
              return new ReturnType(ResultType.FAILURE,
                      "Exception thrown while checking the condition, check its sanctity:\n"
                                      + e.getMessage());
            }
        	 
          String query = queryObject.getQuery();
          query = query.replaceFirst("SET", "using TIMESTAMP "+ v2sTimeStampInMicroseconds(lockReference, System.currentTimeMillis())+ " SET");
      	  queryObject.replaceQueryString(query);
      	  getDSHandle().executePut(queryObject, MusicUtil.CRITICAL);
          long end = System.currentTimeMillis();
          logger.info(EELFLoggerDelegate.applicationLogger,"Time taken for the critical put:" + (end - start) + " ms");
        }catch (MusicQueryException | MusicServiceException | MusicLockingException  e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            return new ReturnType(ResultType.FAILURE,
                            "Exception thrown while doing the critical put\n"
                                            + e.getMessage());
        }
         return new ReturnType(ResultType.SUCCESS, "Update performed");
    }

    
    /**
     * 
     * @param queryObject
     * @param consistency
     * @return Boolean Indicates success or failure
     * @throws MusicServiceException 
     * 
     * 
     */
    public static ResultType nonKeyRelatedPut(PreparedQueryObject queryObject, String consistency) throws MusicServiceException {
        // this is mainly for some functions like keyspace creation etc which does not
        // really need the bells and whistles of Music locking.
        boolean result = false;
        try {
            result = getDSHandle().executePut(queryObject, consistency);
        } catch (MusicQueryException | MusicServiceException ex) {
        	logger.error(EELFLoggerDelegate.errorLogger, ex.getMessage(), AppMessages.UNKNOWNERROR,
                    ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            throw new MusicServiceException(ex.getMessage());
        }
        return result ? ResultType.SUCCESS : ResultType.FAILURE;
    }

    /**
     * This method performs DDL operation on cassandra.
     * 
     * @param queryObject query object containing prepared query and values
     * @return ResultSet
     * @throws MusicServiceException 
     */
    public static ResultSet get(PreparedQueryObject queryObject) throws MusicServiceException {
        ResultSet results = null;
        try {
			results = getDSHandle().executeEventualGet(queryObject);
        } catch (MusicQueryException | MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            throw new MusicServiceException(e.getMessage());
        }
        return results;
    }

    /**
     * This method performs DDL operations on cassandra, if the the resource is available. Lock ID
     * is used to check if the resource is free.
     * 
     * @param keyspace name of the keyspace
     * @param table name of the table
     * @param primaryKeyValue primary key value
     * @param queryObject query object containing prepared query and values
     * @param lockReference lock ID to check if the resource is free to perform the operation.
     * @return ResultSet
     */
    public static ResultSet criticalGet(String keyspace, String table, String primaryKeyValue,
                    PreparedQueryObject queryObject, String lockReference) throws MusicServiceException {
        ResultSet results = null;
        
        try {
            ReturnType result = isTopOfLockStore(keyspace, table, primaryKeyValue, lockReference);
            if(result.getResult().equals(ResultType.FAILURE))
            		return null;//not top of the lock store q
                results = getDSHandle().executeCriticalGet(queryObject);
        } catch (MusicQueryException | MusicServiceException | MusicLockingException e) {
        		logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
        }
        return results;
    }

    /**
     * This method performs DML operation on cassandra, when the lock of the dd is acquired.
     * 
     * @param keyspaceName name of the keyspace
     * @param tableName name of the table
     * @param primaryKey primary key value
     * @param queryObject query object containing prepared query and values
     * @return ReturnType
     * @throws MusicLockingException 
     * @throws MusicServiceException 
     * @throws MusicQueryException 
     */
    public static ReturnType atomicPut(String keyspaceName, String tableName, String primaryKey,
                    PreparedQueryObject queryObject, Condition conditionInfo) throws MusicLockingException, MusicQueryException, MusicServiceException {
        long start = System.currentTimeMillis();
        String fullyQualifiedKey = keyspaceName + "." + tableName + "." + primaryKey;
        String lockReference = createLockReference(fullyQualifiedKey);
        long lockCreationTime = System.currentTimeMillis();
        ReturnType lockAcqResult = acquireLock(fullyQualifiedKey, lockReference);
        long lockAcqTime = System.currentTimeMillis();
        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
            logger.info(EELFLoggerDelegate.applicationLogger,"acquired lock with id " + lockReference);
            ReturnType criticalPutResult = criticalPut(keyspaceName, tableName, primaryKey,
                            queryObject, lockReference, conditionInfo);
            long criticalPutTime = System.currentTimeMillis();
            voluntaryReleaseLock(fullyQualifiedKey,lockReference);
            long lockDeleteTime = System.currentTimeMillis();
            String timingInfo = "|lock creation time:" + (lockCreationTime - start)
                            + "|lock accquire time:" + (lockAcqTime - lockCreationTime)
                            + "|critical put time:" + (criticalPutTime - lockAcqTime)
                            + "|lock delete time:" + (lockDeleteTime - criticalPutTime) + "|";
            criticalPutResult.setTimingInfo(timingInfo);
            return criticalPutResult;
        } else {
            logger.info(EELFLoggerDelegate.applicationLogger,"unable to acquire lock, id " + lockReference);
            voluntaryReleaseLock(fullyQualifiedKey,lockReference);
            return lockAcqResult;
        }
    }
    



    /**
     * This method performs DDL operation on cassasndra, when the lock for the resource is acquired.
     * 
     * @param keyspaceName name of the keyspace
     * @param tableName name of the table
     * @param primaryKey primary key value
     * @param queryObject query object containing prepared query and values
     * @return ResultSet
     * @throws MusicServiceException
     * @throws MusicLockingException 
     * @throws MusicQueryException 
     */
    public static ResultSet atomicGet(String keyspaceName, String tableName, String primaryKey,
                    PreparedQueryObject queryObject) throws MusicServiceException, MusicLockingException, MusicQueryException {
        String fullyQualifiedKey = keyspaceName + "." + tableName + "." + primaryKey;
        String lockReference = createLockReference(fullyQualifiedKey);
        long leasePeriod = MusicUtil.getDefaultLockLeasePeriod();
        ReturnType lockAcqResult = acquireLock(fullyQualifiedKey, lockReference);
        if (lockAcqResult.getResult().equals(ResultType.SUCCESS)) {
            logger.info(EELFLoggerDelegate.applicationLogger,"acquired lock with id " + lockReference);
            ResultSet result =
                            criticalGet(keyspaceName, tableName, primaryKey, queryObject, lockReference);
            voluntaryReleaseLock(fullyQualifiedKey,lockReference);
            return result;
        } else {
            voluntaryReleaseLock(fullyQualifiedKey,lockReference);
            logger.info(EELFLoggerDelegate.applicationLogger,"unable to acquire lock, id " + lockReference);
            return null;
        }
    }
    
    
    public static MusicLockState getMusicLockState(String fullyQualifiedKey) {
    		return null;
    }

    /**
     * authenticate user logic
     * 
     * @param nameSpace
     * @param userId
     * @param password
     * @param keyspace
     * @param aid
     * @param operation
     * @return
     * @throws Exception
     */
    public static Map<String, Object> authenticate(String nameSpace, String userId,
                    String password, String keyspace, String aid, String operation)
                    throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        String uuid = null;
        resultMap = CachingUtil.validateRequest(nameSpace, userId, password, keyspace, aid,
                        operation);
        if (!resultMap.isEmpty())
            return resultMap;
        String isAAFApp = null;
        try {
            isAAFApp= CachingUtil.isAAFApplication(nameSpace);
        } catch(MusicServiceException e) {
           resultMap.put("Exception", e.getMessage());
           return resultMap;
        }
        if(isAAFApp == null) {
            resultMap.put("Exception", "Namespace: "+nameSpace+" doesn't exist. Please make sure ns(appName)"
                    + " is correct and Application is onboarded.");
            return resultMap;
        }
        boolean isAAF = Boolean.valueOf(isAAFApp);
        if (userId == null || password == null) {
        	logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
            logger.error(EELFLoggerDelegate.errorLogger,"One or more required headers is missing. userId: " + userId
                            + " :: password: " + password);
            resultMap.put("Exception",
                            "UserId and Password are mandatory for the operation " + operation);
            return resultMap;
        }
        if(!isAAF && !(operation.equals("createKeySpace"))) {
            resultMap = CachingUtil.authenticateAIDUser(nameSpace, userId, password, keyspace);
            if (!resultMap.isEmpty())
                return resultMap;
            
        }
        if (isAAF && nameSpace != null && userId != null && password != null) {
            boolean isValid = true;
            try {
            	 isValid = CachingUtil.authenticateAAFUser(nameSpace, userId, password, keyspace);
            } catch (Exception e) {
            	logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                logger.error(EELFLoggerDelegate.errorLogger,"Got exception while AAF authentication for namespace " + nameSpace);
                resultMap.put("Exception", e.getMessage());
            }
            if (!isValid) {
            	logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
                resultMap.put("Exception", "User not authenticated...");
            }
            if (!resultMap.isEmpty())
                return resultMap;

        }

        if (operation.equals("createKeySpace")) {
            logger.info(EELFLoggerDelegate.applicationLogger,"AID is not provided. Creating new UUID for keyspace.");
            PreparedQueryObject pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                            "select uuid from admin.keyspace_master where application_name=? and username=? and keyspace_name=? allow filtering");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), nameSpace));
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                            MusicUtil.DEFAULTKEYSPACENAME));

            try {
                Row rs = MusicCore.get(pQuery).one();
                uuid = rs.getUUID("uuid").toString();
                resultMap.put("uuid", "existing");
            } catch (Exception e) {
                logger.info(EELFLoggerDelegate.applicationLogger,"No UUID found in DB. So creating new UUID.");
                uuid = CachingUtil.generateUUID();
                resultMap.put("uuid", "new");
            }
            resultMap.put("aid", uuid);
        }

        return resultMap;
    }
    
    /**
     * @param lockName
     * @return
     */
    public static Map<String, Object> validateLock(String lockName) {
        Map<String, Object> resultMap = new HashMap<>();
        String[] locks = lockName.split("\\.");
        if(locks.length < 3) {
            resultMap.put("Exception", "Invalid lock. Please make sure lock is of the type keyspaceName.tableName.primaryKey");
            return resultMap;
        }
        String keyspace= locks[0];
        if(keyspace.startsWith("$"))
            keyspace = keyspace.substring(1);
        resultMap.put("keyspace",keyspace);
        return resultMap;
    }
    
    
	/**
	 * Given the time of write for an update in a critical section, this method provides a transformed timestamp
	 * that ensures that a previous lock holder who is still alive can never corrupt a later critical section.
	 * The main idea is to us the lock reference to clearly demarcate the timestamps across critical sections. 
	 * @param the UUID lock reference associated with the write. 
	 * @param the long timeOfWrite which is the actual time at which the write took place 
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */	
	private static long v2sTimeStampInMicroseconds(String lockReference, long timeOfWrite) throws MusicServiceException, MusicQueryException{
        long lockEpochMillis = Long.parseLong(lockReference);

        long lockEternityMillis = lockEpochMillis - MusicUtil.MusicEternityEpochMillis;

        long ts = lockEternityMillis * MusicUtil.MaxCriticalSectionDurationMillis
                + (timeOfWrite - lockEpochMillis);

        return ts;

//		long test = (lockReferenceUUID.timestamp()-MusicUtil.MusicEternityEpochMillis);
//		long timeStamp = (lockReferenceUUID.timestamp()-MusicUtil.MusicEternityEpochMillis)*MusicUtil.MaxCriticalSectionDurationMillis
//				+timeOfWrite;
//		return timeStamp;

//		return timeOfWrite*1000;
	}

	public static void main(String[] args) {
		String x = "axe top";
		x = x.replaceFirst("top", "sword");
		System.out.print(x); //returns sword pickaxe
	}
}
