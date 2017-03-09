package com.att.research.music.unittests;

import org.apache.log4j.Logger;

import com.att.research.music.lockingservice.MusicLockingService;

public class TestLockStore {
	final static Logger logger = Logger.getLogger(TestLockStore.class);

	public static void main(String[] args) throws Exception {
		String lockName = "/achristmkllas";
		MusicLockingService ml = new MusicLockingService();
		ml.deleteLock(lockName);


		logger.info("lockname:"+lockName);	
			
		String lockId1 = ml.createLockId(lockName);
		logger.info("lockId1 "+ lockId1);
        logger.info(ml.isMyTurn(lockId1));
        
		String lockId2 = ml.createLockId(lockName);
		logger.info("lockId2 "+lockId2);
		logger.info("check "+ml.isMyTurn("$bank$x-94608776321630264-0000000000"));
		logger.info(ml.isMyTurn(lockId2));

		//zkClient.unlock(lockId1);
		//logger.info(ml.lock(lockId2));
		//zkClient.unlock(lockId2);
 	}


}
