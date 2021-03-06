/*
 * 
This licence applies to all files in this repository unless otherwise specifically
stated inside of the file. 

 ---------------------------------------------------------------------------
   Copyright (c) 2016 AT&T Intellectual Property

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at:

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ---------------------------------------------------------------------------

 */
package com.att.research.music.zklockingservice;


import java.io.IOException;
import java.util.StringTokenizer;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.att.research.music.main.MusicUtil;
public class MusicLockingService implements Watcher {

	private  final int SESSION_TIMEOUT = 180000;
	ZkStatelessLockService zkLockHandle= null;
	private  CountDownLatch connectedSignal = new CountDownLatch(1);
	
	public MusicLockingService(){
		try {
			ZooKeeper zk = new ZooKeeper(MusicUtil.myZkHost, SESSION_TIMEOUT, this);
			connectedSignal.await();
			zkLockHandle = new ZkStatelessLockService(zk);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public ZkStatelessLockService getzkLockHandle(){
		return zkLockHandle;
	}
	
	public MusicLockingService(String lockServer){
		try {
			ZooKeeper zk = new ZooKeeper(lockServer, SESSION_TIMEOUT, this);
			connectedSignal.await();
			zkLockHandle = new ZkStatelessLockService(zk);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String createLockReference(String lockName){
	//	createLockIfItDoesNotExist(lockName);
		String lockIdWithSlash = zkLockHandle.createLockId(lockName);
		return lockIdWithSlash.replace('/', '$');
	}
	
	public boolean isMyTurn(String lockIdWithDollar){
		String lockId = lockIdWithDollar.replace('$', '/');
		StringTokenizer st = new StringTokenizer(lockId);
		String lockName = "/"+st.nextToken("/");
		try {
			return zkLockHandle.lock(lockName, lockId);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public void releaseLockByDeletingReference(String lockIdWithDollar){
		String lockId = lockIdWithDollar.replace('$', '/');
		zkLockHandle.unlock(lockId);
	}

	public void deleteLock(String lockName){
		zkLockHandle.deleteLock(lockName);	
	}
	
	public String whoseTurnIsIt(String lockName){
		String lockHolder = zkLockHandle.currentLockHolder(lockName);
		String lockHolderWithDollar = lockHolder.replace('/', '$');
		return lockHolderWithDollar;	
	}
	
	public void process(WatchedEvent event) { // Watcher interface
		if (event.getState() == KeeperState.SyncConnected) {
			connectedSignal.countDown();
		}
	}


	
	public boolean lockIdExists(String lockIdWithDollar) {
		String lockId = lockIdWithDollar.replace('$', '/');
		return zkLockHandle.checkIfLockExists(lockId);
	}
	
}
