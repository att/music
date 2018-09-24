package org.onap.music.datastore;

import java.util.UUID;

import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.UUIDs;

/*
 * This is the lock store that is built on top of Cassandra that is used by MUSIC to maintain lock state. 
 */

public class CassaLockStore {
	
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(CassaLockStore.class);
	
	public class LockObject{
		public UUID lockRef;
		public String createTime;
		public String acquireTime;
		public LockObject(UUID lockRef, String createTime, 	String acquireTime) {
			this.lockRef = lockRef;
			this.acquireTime = acquireTime;
			this.createTime = createTime;
			
		}
	}
	CassaDataStore dsHandle;
	public CassaLockStore() {
		dsHandle = new CassaDataStore();
	}
	
	public CassaLockStore(CassaDataStore dsHandle) {
		this.dsHandle=dsHandle;
	}

    
	/**
	 * 
	 * This method creates a shadow locking table for every main table in Cassandra. This table tracks all information regarding locks. 
	 * @param keyspace of the application. 
	 * @param table of the application. 
	 * @return true if the operation was successful.
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */
	public boolean createLockQueue(String keyspace, String table) throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Create lock queue/table for " +  keyspace+"."+table);
		table = "lockQ_"+table; 
		String tabQuery = "CREATE TABLE IF NOT EXISTS "+keyspace+"."+table
				+ " ( key text, lockReference timeuuid, createTime text, acquireTime text,   PRIMARY KEY ((key), lockReference) ) "
				+ "WITH CLUSTERING ORDER BY (lockReference ASC);";
		System.out.println(tabQuery);
		PreparedQueryObject queryObject = new PreparedQueryObject(); 
		
		queryObject.appendQueryString(tabQuery);
		boolean result = false;
		result = dsHandle.executePut(queryObject, "eventual");
		return result;
	}
	
	
	
	
	/**
	 * This method creates a lock reference for each invocation. The lock references are monotonically increasing UUIDs. 
	 * @param keyspace of the application. 
	 * @param table of the application. 
	 * @param key is the primary key of the application table
	 * @return the UUID lock reference.
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */
	public String genLockRefandEnQueue(String keyspace, String table, String key) throws MusicServiceException, MusicQueryException {
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Create lock reference  for " +  keyspace+"."+table+"."+key);
		table = "lockQ_"+table; 
		UUID timeBasedUuid = UUIDs.timeBased();
        PreparedQueryObject queryObject = new PreparedQueryObject();
		String values = "(?,?,?,?)";
		queryObject.addValue(key);
		queryObject.addValue(timeBasedUuid);
		queryObject.addValue(timeBasedUuid.timestamp()+"");
		queryObject.addValue("0");
		String insQuery = "INSERT INTO "+keyspace+"."+table+"(key, lockReference, createTime, acquireTime) VALUES"+values+" IF NOT EXISTS;";	
        queryObject.appendQueryString(insQuery);
        dsHandle.executePut(queryObject, "critical");	
		return timeBasedUuid+"";
	}
	
	
	/**
	 * This method returns the top of lock table/queue for the key. 
	 * @param keyspace of the application. 
	 * @param table of the application. 
	 * @param key is the primary key of the application table
	 * @return the UUID lock reference.
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */
	public LockObject peekLockQueue(String keyspace, String table, String key) throws MusicServiceException, MusicQueryException{
        logger.info(EELFLoggerDelegate.applicationLogger,
                "Peek in lock table for " +  keyspace+"."+table+"."+key);
		table = "lockQ_"+table; 
		String selectQuery = "select * from "+keyspace+"."+table+" where key='"+key+"' LIMIT 1;";	
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(selectQuery);
		ResultSet results = dsHandle.executeEventualGet(queryObject);
		Row row = results.one();
		UUID lockReference = row.getUUID("lockReference");
		String createTime = row.getString("createTime");
		String acquireTime = row.getString("acquireTime");

		return new LockObject(lockReference, createTime,acquireTime);
	}
	
	
	/**
	 * This method removes the lock ref from the lock table/queue for the key. 
	 * @param keyspace of the application. 
	 * @param table of the application. 
	 * @param key is the primary key of the application table
	 * @param the UUID lock reference that needs to be dequeued. 
	 * @throws MusicServiceException
	 * @throws MusicQueryException
	 */	
	public void deQueueLockRef(String keyspace, String table, String key, String lockReference) throws MusicServiceException, MusicQueryException{
		table = "lockQ_"+table; 
        PreparedQueryObject queryObject = new PreparedQueryObject();
        String deleteQuery = "delete from "+keyspace+"."+table+" where key='"+key+"' AND lockReference ="+lockReference+" IF EXISTS;";	
        queryObject.appendQueryString(deleteQuery);
		dsHandle.executePut(queryObject, "critical");	
	}
	

	public void updateLockAcquireTime(String keyspace, String table, String key, String lockReference) throws MusicServiceException, MusicQueryException{
		table = "lockQ_"+table; 
        PreparedQueryObject queryObject = new PreparedQueryObject();
        String updateQuery = "update "+keyspace+"."+table+" set acquireTime='"+ System.currentTimeMillis()+"' where key='"+key+"' AND lockReference ="+lockReference+" IF EXISTS;";	
        queryObject.appendQueryString(updateQuery);
		dsHandle.executePut(queryObject, "eventual");	

	}
	

}
