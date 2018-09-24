package org.onap.music.unittests;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.onap.music.datastore.CassaLockStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;

public class TestCassaLockStore {
	
	
	public static void main(String[] args) {
		
		
		try {
			CassaLockStore lockStore = new CassaLockStore();
			String keyspace = "ks_testLockStore";
			String table = "table_testLockStore";

			Map<String,Object> replicationInfo = new HashMap<String, Object>();
            replicationInfo.put("'class'", "'SimpleStrategy'");
            replicationInfo.put("'replication_factor'", 1);
            
            PreparedQueryObject queryObject = new PreparedQueryObject();
            queryObject.appendQueryString("CREATE KEYSPACE " + keyspace + " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));
            MusicCore.nonKeyRelatedPut(queryObject, "eventual");

            
            queryObject = new PreparedQueryObject();
            queryObject.appendQueryString("CREATE TABLE " + keyspace + "." + table + " (name text PRIMARY KEY, count varint);");
            MusicCore.createTable(keyspace, table, queryObject, "eventual");              

			
			lockStore.createLockQueue(keyspace, table);
			
			String lockRefb1 = lockStore.genLockRefandEnQueue(keyspace, table, "bharath");
			
			
			
			String lockRefc1 = lockStore.genLockRefandEnQueue(keyspace, table, "cat");

			String lockRefc2 = lockStore.genLockRefandEnQueue(keyspace, table, "cat");


			assert(lockStore.peekLockQueue(keyspace, table, "cat").equals(lockRefc1));
			
			assert(!lockStore.peekLockQueue(keyspace, table, "cat").equals(lockRefc2));

			
			assert(lockStore.peekLockQueue(keyspace, table, "bharath").equals(lockRefb1));
			
			
			lockStore.deQueueLockRef(keyspace, table, "cat", lockRefc1);
			
			assert(lockStore.peekLockQueue(keyspace, table, "cat").equals(lockRefc2));

		} catch (MusicServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MusicQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
