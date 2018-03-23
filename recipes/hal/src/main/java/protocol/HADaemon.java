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
package protocol;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


import musicinterface.MusicHandle;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.att.eelf.logging.EELFLoggerDelegate;


public class HADaemon {
	
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(HADaemon.class);
	
	String id;
	String lockName,lockRef;
	public  enum CoreState {PASSIVE, ACTIVE};
	public  enum ScriptResult {ALREADY_RUNNING, SUCCESS_RESTART, FAIL_RESTART};
	String keyspaceName;
	String tableName;
	
	public HADaemon(String id){
		this.id = id; 
		bootStrap();
	}
	
	private void bootStrap(){
		logger.info(EELFLoggerDelegate.applicationLogger, "Bootstrapping this site daemon");
		keyspaceName = "hal_"+ConfigReader.getConfigAttribute("appName");
		MusicHandle.createKeyspaceEventual(keyspaceName);
		
		tableName = "Replicas";
		Map<String,String> replicaFields = new HashMap<String,String>();
		replicaFields.put("id", "text");
		replicaFields.put("isactive", "boolean");
		replicaFields.put("timeoflastupdate", "varint");
		replicaFields.put("lockref", "text");
		replicaFields.put("PRIMARY KEY", "(id)");

		MusicHandle.createTableEventual(keyspaceName, tableName, replicaFields);	
		MusicHandle.createIndexInTable(keyspaceName, tableName, "lockref");
		
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("id",this.id);
		values.put("isactive","false");
		values.put("timeoflastupdate", "0");
		values.put("lockref", "");
		MusicHandle.insertIntoTableEventual(keyspaceName, tableName, values);
		MusicHandle.insertIntoTableEventual(keyspaceName, tableName, values);
		
		lockName = keyspaceName+".active";
	}
	
	/**
	 * @deprecated
	 */
	private String getLockRef(String replicaId){	
		//first check if a lock reference exists for this id..
		Map<String,Object> replicaDetails = MusicHandle.readSpecificRow(keyspaceName, tableName, "id", replicaId); 
		if(replicaDetails == null){
			logger.info(EELFLoggerDelegate.applicationLogger, "No entry found in MUSIC Replicas table for this daemon...");
			return null; 
		}
		logger.info(EELFLoggerDelegate.applicationLogger, "Entry found:"+replicaDetails.get("lockref"));
		return (String)replicaDetails.get("lockref");
	}
	
	
	/**
	 * This function maintains the key invariant that it will return true for only one id
	 * @return true if this replica is current lock holder
	 */
	private boolean isActiveLockHolder(){
		logger.info(EELFLoggerDelegate.applicationLogger, "isActiveLockHolder");
		boolean isLockHolder = acquireLock();
		if (isLockHolder) {//update active table
			logger.info(EELFLoggerDelegate.applicationLogger, "Daemon is the current activeLockHolder");
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("isactive","true");
			values.put("id",this.id);
			MusicHandle.insertIntoTableEventual(keyspaceName, tableName, values);
		}
		return isLockHolder;
	}
	
	/**
	 * tries to acquire lockRef
	 * if lockRef no longer exists creates a new lock and updates locally
	 * @return true if active lock holder, false otherwise
	 */
	private boolean acquireLock() {
		logger.info(EELFLoggerDelegate.applicationLogger, "acquireLock");
		if (lockRef==null) return false;
		Map<String, Object> result = MusicHandle.acquireLock(lockRef);
		Map<String, Object> lockMap = (Map<String, Object>) result.get("lock");
		if (lockMap.getOrDefault("message", "Lockid doesn't exist").equals("Lockid doesn't exist")) {
			logger.info(EELFLoggerDelegate.applicationLogger,
					"Lockref " + lockRef + " doesn't exist, getting new lockref");
			lockRef = MusicHandle.createLockRef(lockName);
			logger.info(EELFLoggerDelegate.applicationLogger, "This site's new reference is " + lockRef);
			result = MusicHandle.acquireLock(lockRef);
		}
		logger.info(EELFLoggerDelegate.applicationLogger, "result of acquiring lock " + result.get("status"));
		return (result.get("status").equals("SUCCESS")?true:false);
	}
	
	
	/**
	 * The main startup function for each daemon
	 * @param startPassive dictates whether the node should start in an passive mode
	 */
	private void startHAFlow(boolean startPassive){
		logger.info(EELFLoggerDelegate.applicationLogger, "startHAFlow"+startPassive);
		if (startPassive) {
			startAsPassiveReplica();
		}
		
		lockRef = MusicHandle.createLockRef(lockName);	

		while (true) {
			if (isActiveLockHolder()) {
				activeFlow();
			}
			else {
				passiveFlow();
			}
		}
	}

	/**
	 * Waits until there is an active, running replica
	 */
	private void startAsPassiveReplica() {
		logger.info(EELFLoggerDelegate.applicationLogger,
				"Starting in 'passive mode'. Checking to see if active has started");
		String activeLockRef = MusicHandle.whoIsLockHolder(lockName);
		Map<String,Object> active = getReplicaDetails(activeLockRef);
		
		while (active==null || !(Boolean)active.getOrDefault("isactive", "false")
				 || !isReplicaAlive((String)active.get("id"))) {
			activeLockRef = MusicHandle.whoIsLockHolder(lockName);
			active = getReplicaDetails(activeLockRef);
			//back off if needed
			try {
				Long sleeptime = Long.parseLong(ConfigReader.getConfigAttribute("core-monitor-sleep-time", "1000"));
				if (sleeptime>0) {
					logger.info(EELFLoggerDelegate.applicationLogger, "Sleeping for " + sleeptime + " ms");
					Thread.sleep(sleeptime);
				}
			} catch (Exception e) {
					logger.error(e.getMessage());
			}
			
		}
		logger.info(EELFLoggerDelegate.applicationLogger, 
				"Active site id=" + active.get("id") + " has started. Continuing in passive mode");
	}
	
	/**
	 * Make sure that the replica you are monitoring is running by running
	 * the script provided.
	 * 
	 * Try to run the script noOfRetryAttempts times, as defined by the HAL configuration.
	 * This function will wait in between retry attempts, as determined by 'restart-backoff-time' 
	 * defined in HAL configuration file (immediate retry is default, if no value is provided)
	 * 
	 * @param script script to be run
	 * @return ScriptResult based off scripts response
	 */
	private ScriptResult tryToEnsureCoreFunctioning(ArrayList<String> script){
		logger.info(EELFLoggerDelegate.applicationLogger, "tryToEnsureCoreFunctioning");
		int noOfAttempts = Integer.parseInt(ConfigReader.getConfigAttribute("noOfRetryAttempts"));
		ScriptResult result = ScriptResult.FAIL_RESTART;
		
		while (noOfAttempts > 0) {			
			result = HalUtil.executeBashScriptWithParams(script);
			if (result == ScriptResult.ALREADY_RUNNING) {
				logger.info(EELFLoggerDelegate.applicationLogger,
						"Executed core script, the core was already running");
				return result;
			} else if (result == ScriptResult.SUCCESS_RESTART) {
				logger.info(EELFLoggerDelegate.applicationLogger, 
						"Executed core script, the core had to be restarted");
				return result;
			} else if (result == ScriptResult.FAIL_RESTART) {
				noOfAttempts--;
				logger.info(EELFLoggerDelegate.applicationLogger, 
						"Executed core script, the core could not be re-started, retry attempts left ="+noOfAttempts);
			}
			//backoff period in between restart attempts
			try {
				Thread.sleep(Long.parseLong(ConfigReader.getConfigAttribute("restart-backoff-time", "0")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		logger.info(EELFLoggerDelegate.applicationLogger,
				"Tried enough times and still unable to start the core, giving up lock and starting passive flow..");
		return result;
	}
	
	/**
	 * Update this replica's lockRef and update the heartbeat in replica table
	 */
	private void updateHealth(CoreState isactive) {
		logger.info(EELFLoggerDelegate.applicationLogger, "updateHealth " +isactive);
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("id",this.id);
		values.put("timeoflastupdate", System.currentTimeMillis());
		values.put("lockref", this.lockRef);
		values.put("isactive",isactive==CoreState.ACTIVE?true:false);
		MusicHandle.insertIntoTableEventual(keyspaceName, tableName, values);
	}
	
	/**
	 * Checks to see if the replica is alive
	 * @param id the id of the replica to check if is alive
	 * @return
	 */
	private boolean isReplicaAlive(String id){
		logger.info(EELFLoggerDelegate.applicationLogger, "isReplicaAlive " + id);
		Map<String,Object> valueMap = MusicHandle.readSpecificRow(keyspaceName, tableName, "id", id);
		if (valueMap == null) {
			logger.info(EELFLoggerDelegate.applicationLogger, "No entry showing...");
			return false; 
		}
		
		if (!valueMap.containsKey("timeoflastupdate") || valueMap.get("timeoflastupdate")==null) {
			logger.info(EELFLoggerDelegate.applicationLogger, "No 'timeoflastupdate' entry showing...");
			return false;
		}

		long lastUpdate = (Long)valueMap.get("timeoflastupdate");
		logger.info(EELFLoggerDelegate.applicationLogger, "time of last update:"+lastUpdate);
	    long timeOutPeriod = Long.parseLong(ConfigReader.getConfigAttribute("hal-timeout"));
	    long currentTime = System.currentTimeMillis();
	    logger.info(EELFLoggerDelegate.applicationLogger, "current time:"+currentTime);
	    long timeSinceUpdate = currentTime-lastUpdate; 
	    logger.info(EELFLoggerDelegate.applicationLogger, "time since update:"+timeSinceUpdate);
	    if(timeSinceUpdate > timeOutPeriod)
	    	return false;
	    else
	    	return true;
	}
	
	private Map<String, Object> getReplicaDetails(String lockRef){
		return MusicHandle.readSpecificRow(keyspaceName, tableName, "lockref", lockRef);
	}

	/**
	 * Releases lock and ensures replica id's 'isactive' state to false
	 * @param lockRef
	 */
	private void releaseLock(String lockRef){
		logger.info(EELFLoggerDelegate.applicationLogger, "releaseLock " + lockRef);
		if(lockRef == null){
			logger.info(EELFLoggerDelegate.applicationLogger, "There is no lock entry..");
			return;
		}
			
		if(lockRef.equals("")){
			logger.info(EELFLoggerDelegate.applicationLogger, "Already unlocked..");
			return;
		}
		
		Map<String, Object> replicaDetails = getReplicaDetails(lockRef);
		String replicaId = "UNKNOWN";
		if (replicaDetails!=null) {
			replicaId = (String)replicaDetails.get("id");
		}
		
		
		logger.info(EELFLoggerDelegate.applicationLogger, "Unlocking hal "+replicaId + " with lockref"+ lockRef);
		MusicHandle.unlock(lockRef);
		logger.info(EELFLoggerDelegate.applicationLogger, "Unlocked hal "+replicaId);
		if (replicaId.equals(this.id)) { //if unlocking myself, remove reference to lockref
			this.lockRef=null;
		}
		
		if (replicaId.equals("UNKNOWN")) {
			return;
		}
		//create entry in replicas table
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("isactive",false);		
		values.put("lockref", "");
		MusicHandle.updateTableEventual(keyspaceName, tableName, "id", replicaId, values);
	}
	
	private void tryToEnsurePeerHealth(){
		ArrayList<String> replicaList =  ConfigReader.getConfigListAttribute(("replicaIdList"));
		for (Iterator<String> iterator = replicaList.iterator(); iterator.hasNext();) {
			String replicaId = (String) iterator.next();
			if(replicaId.equals(this.id) == false){
				if(isReplicaAlive(replicaId) == false){
					//restart if suspected dead
					//releaseLock(replicaId);
					//Don't hold up main thread for restart
					Runnable restartThread = new RestartThread(replicaId);
					new Thread(restartThread).start();
					
					logger.info(EELFLoggerDelegate.applicationLogger,
							lockRef + " status: "+MusicHandle.acquireLock(lockRef));
				}
			}
		}	
	}
	
	private boolean restartHALDaemon(String replicaId, int noOfAttempts){
		logger.info(EELFLoggerDelegate.applicationLogger, "Hal Daemon--"+replicaId+"--needs to be restarted");

		ArrayList<String> restartScript = ConfigReader.getExeCommandWithParams("restart-hal-"+replicaId);
		if (restartScript!=null && restartScript.size()>0 && restartScript.get(0).length()>0) {
			HalUtil.executeBashScriptWithParams(restartScript);
		}
		return true;//need to find a way to check if the script is running. Just check if process is running maybe? 
/*
		boolean result = false;
		while(result == false){
			ArrayList<String> restartScript = ConfigReader.getExeCommandWithParams("restart-hal-"+id);
			HalUtil.executeBashScriptWithParams(restartScript);
			result = Boolean.parseBoolean(resultString);
			noOfAttempts--;
			if(noOfAttempts <= 0)
				break; 
		}
		return result; 
*/	}
	
	/**
	 * Give current active sufficient time (as defined by configured 'hal-timeout' value) to become passive.
	 * If current active does not become passive in the configured amount of time, the current active site
	 * is forcibly reset to a passive state.
	 * 
	 * This method should only be called after the lock of the previous active is released and this 
	 * replica has become the new active
	 * 
	 * @param currentActiveId
	 */
	private void takeOverFromCurrentActive(String currentActiveLockRef){
		if (currentActiveLockRef==null || currentActiveLockRef.equals(this.lockRef)) {
			return;
		}
		
		long startTime = System.currentTimeMillis();
		long restartTimeout = Long.parseLong(ConfigReader.getConfigAttribute("hal-timeout"));
		while(true){
			Map<String,Object> replicaDetails = getReplicaDetails(currentActiveLockRef);
			if (replicaDetails==null || !replicaDetails.containsKey("isactive") ||
					!(Boolean)replicaDetails.get("isactive")) {
				break;
			}
			
			//waited long enough..just make the old active passive yourself
			if ((System.currentTimeMillis() - startTime) > restartTimeout) {
				logger.info(EELFLoggerDelegate.applicationLogger, 
						"Old Active not responding..resetting Music state of old active to passive myself");
				Map<String, Object> removeActive = new HashMap<String,Object>();
				removeActive.put("isactive", false);
				MusicHandle.updateTableEventual(keyspaceName, tableName, "lockref", currentActiveLockRef, removeActive);
				break;
			}
			//make sure we don't time out while we wait
			updateHealth(CoreState.PASSIVE);
		}

		logger.info(EELFLoggerDelegate.applicationLogger,
				"Old Active has now become passive, so starting active flow ***");

		//now you can take over as active! 
	}
	
	
	private void activeFlow(){
		logger.info(EELFLoggerDelegate.applicationLogger, "activeFlow");
		while (true) {
			if(acquireLock() == false){
				logger.info(EELFLoggerDelegate.applicationLogger, "I no longer have the lock! Make myself passive");
				return;
			}
			
			ScriptResult result = tryToEnsureCoreFunctioning(ConfigReader.getExeCommandWithParams("ensure-active-"+id));
			if (result == ScriptResult.ALREADY_RUNNING) {
				//do nothing
			} else if (result == ScriptResult.SUCCESS_RESTART) {
				//do nothing
			} else if (result == ScriptResult.FAIL_RESTART) {//unable to start core, just give up and become passive
				releaseLock(lockRef);
				return;
			}
			
			updateHealth(CoreState.ACTIVE);
			
			logger.info(EELFLoggerDelegate.applicationLogger, 
					"--(Active) Hal Daemon--"+id+"---CORE ACTIVE---Lock Ref:"+lockRef);

			tryToEnsurePeerHealth();
			logger.info(EELFLoggerDelegate.applicationLogger, 
					"--(Active) Hal Daemon--"+id+"---PEERS CHECKED---");

			//back off if needed
			try {
				Long sleeptime = Long.parseLong(ConfigReader.getConfigAttribute("core-monitor-sleep-time", "0"));
				if (sleeptime>0) {
					logger.info(EELFLoggerDelegate.applicationLogger, "Sleeping for " + sleeptime + " ms");
					Thread.sleep(sleeptime);
				}
			} catch (Exception e) {
					logger.error(e.getMessage());
			}
		}
	}
	
	private void passiveFlow(){
		logger.info(EELFLoggerDelegate.applicationLogger, "passiveFlow");
		while(true){
			ScriptResult result = tryToEnsureCoreFunctioning(ConfigReader.getExeCommandWithParams("ensure-passive-"+id));		
			if (result == ScriptResult.ALREADY_RUNNING) {
				if (lockRef==null) {
					logger.info(EELFLoggerDelegate.applicationLogger,
							"Replica does not have a lock, but is running. Getting a lock now");
					lockRef = MusicHandle.createLockRef(lockName);
					logger.info(EELFLoggerDelegate.applicationLogger, "new lockRef " + lockRef);
				}
			} else if (result == ScriptResult.SUCCESS_RESTART) {
				//we can now handle being after, put yourself back in queue
				logger.info(EELFLoggerDelegate.applicationLogger, "Script successfully restarted. Getting a new lock");
				lockRef = MusicHandle.createLockRef(lockName);
				logger.info(EELFLoggerDelegate.applicationLogger, "new lockRef " + lockRef);
			} else if (result == ScriptResult.FAIL_RESTART) {
				logger.info(EELFLoggerDelegate.applicationLogger,
						"Site not working and could not restart, releasing lock"); 
				releaseLock(lockRef);
			}
			
			//update own health in music
			updateHealth(CoreState.PASSIVE);
			
			logger.info(EELFLoggerDelegate.applicationLogger,
					"-- {Passive} Hal Daemon--"+id+"---CORE PASSIVE---Lock Ref:"+lockRef);

			//obtain active lock holder's id
			String activeLockRef = MusicHandle.whoIsLockHolder(lockName);
			releaseLockIfActiveIsDead(activeLockRef);
			
			if (isActiveLockHolder()) {
				logger.info(EELFLoggerDelegate.applicationLogger,
						"***I am the active lockholder, so taking over from previous active***");
				takeOverFromCurrentActive(activeLockRef);
				return;
			}

			//back off if needed
			try {
				Long sleeptime = Long.parseLong(ConfigReader.getConfigAttribute("core-monitor-sleep-time", "0"));
				if (sleeptime>0) {
					logger.info(EELFLoggerDelegate.applicationLogger, "Sleeping for " + sleeptime + " ms");
					Thread.sleep(sleeptime);
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
	}

	/**
	 * Releases the lock if the active lock holder is dead, not responsive, or cannot be found.
	 * @param activeLockRef
	 * @return the active id
	 */
	private void releaseLockIfActiveIsDead(String activeLockRef) {
		logger.info(EELFLoggerDelegate.applicationLogger, "releaseLockIfActiveIsDead " + activeLockRef);
		Map<String, Object> activeDetails =  getReplicaDetails(activeLockRef);
		Boolean activeIsAlive = false;
		String activeId = null;
		if (activeDetails!=null) {
			activeId = (String)activeDetails.get("id");
			logger.info(EELFLoggerDelegate.applicationLogger, "Active lockholder is site " + activeId);
			activeIsAlive = isReplicaAlive(activeId);
		}

		if (activeIsAlive == false) {
			logger.info(EELFLoggerDelegate.applicationLogger, "Active lockholder is not alive");
			if (activeId==null) {
				if (activeLockRef!=null && !activeLockRef.equals("")) {
					//no reference to the current lock, probably corrupt/stale data
					logger.info(EELFLoggerDelegate.applicationLogger,
							"Unknown active lockholder. Releasing current lock");
					MusicHandle.unlock(activeLockRef);
				} else {
					logger.info(EELFLoggerDelegate.applicationLogger,
							"*****No lock holders. Make sure there are healthy sites*****");
				}
			} else {
				logger.info(EELFLoggerDelegate.applicationLogger,
						"Active " + activeId + " is suspected dead. Releasing it's lock.");
				releaseLock(activeLockRef);
			}
		}
	}
	
	private class RestartThread implements Runnable{
		String replicaId;
		public RestartThread(String replicaId) {
			this.replicaId = replicaId;
		}
		public void run() {
			restartHALDaemon(this.replicaId, 1);
		}
	}
	
	public static void main(String[] args){
		Options opts = new Options();
		
		Option idOpt = new Option("i", "id", true, "hal identifier number");
		idOpt.setRequired(true);
		opts.addOption(idOpt);
		
		Option passive = new Option("p", "passive", false, "start hal in passive mode (default false)");
		opts.addOption(passive);
		
		Option config = new Option("c", "config", true, "location of config.json file (default same directory as hal jar)");
		opts.addOption(config);
		
		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;
		try {
			cmd = parser.parse(opts, args);
		} catch (ParseException e) {
			e.printStackTrace();
			formatter.printHelp("hal", opts);
			System.exit(1);
			return;
		}
		
		String id = cmd.getOptionValue("id");
		boolean startPassive = false;
		if (cmd.hasOption("passive")) {
			startPassive = true;
		}
		if (cmd.hasOption("c")) {
			ConfigReader.setConfigLocation(cmd.getOptionValue("c"));
		}

		System.out.println("--Hal Daemon version "+HalUtil.version+"--replica id "+id+"---START---"+(startPassive?"passive":"active"));
		HADaemon hd = new HADaemon(id);
		hd.startHAFlow(startPassive);
	}

}
