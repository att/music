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

public class HADaemon {
	String id;
	String lockName,lockRef;
	public  enum CoreState {PASSIVE, ACTIVE};
	public  enum ScriptResult {ALREADY_RUNNING, SUCCESS_RESTART, FAIL_RESTART};
	String coreName;
	public HADaemon(String id){
		this.id = id; 
		bootStrap();
	}
	
	private void bootStrap(){
		coreName = "hal_"+ConfigReader.getConfigAttribute("appName");
		String keyspaceName=coreName;
		MusicHandle.createKeyspaceEventual(keyspaceName);
		
		String tableName = "Replicas";
		Map<String,String> replicaFields = new HashMap<String,String>();
		replicaFields.put("id", "text");
		replicaFields.put("isActive", "boolean");
		replicaFields.put("TimeOfLastUpdate", "varint");
		replicaFields.put("lockRef", "text");
		replicaFields.put("PRIMARY KEY", "(id)");

		MusicHandle.createTableEventual(keyspaceName, tableName, replicaFields);	
		
		String activeTableName = "ActiveDetails";
		Map<String,String> activeTableFields = new HashMap<String,String>();
		activeTableFields.put("active", "text");
		activeTableFields.put("id", "text");
		activeTableFields.put("PRIMARY KEY", "(active)");
		MusicHandle.createTableEventual(keyspaceName, activeTableName, activeTableFields);	
		
		
		lockName = coreName+".active";
		createLockRefIfDoesNotExist();

	}
	
	private void createLockRefIfDoesNotExist(){
		//is this to ensure preference to a previously acquired
		System.out.println("Checking if any lock reference already exists for this object in MUSIC...");
		String oldLockRef = getLockRef(this.id);
		if((oldLockRef == null) || (oldLockRef.equals(""))){
			System.out.println("No prior lock reference..");
			lockRef = MusicHandle.createLockRef(lockName);
			updateHealth(this.id, CoreState.PASSIVE);
		}
		else{
			System.out.println("Lock reference already exists");
			lockRef = oldLockRef;
		}
		
	}
	
	private String getLockRef(String replicaId){	
		//first check if a lock reference exists for this id..
		Map<String,Object> replicaDetails = MusicHandle.readSpecificRow(coreName, "Replicas", "id", replicaId); 
		if(replicaDetails == null){
			System.out.println("No entry found in MUSIC Replicas table for this daemon...");
			return null; 
		}
		System.out.println("Entry found:"+replicaDetails.get("lockref"));
		return (String)replicaDetails.get("lockref");
	}
	
	//this function maintains the key invariant that it will return true for only one id
	private boolean isActiveLockHolder(){
		boolean isLockHolder = MusicHandle.acquireLock(lockRef);
		if(isLockHolder){//update active table
			System.out.println("Daemon is the current lock holder!...");
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("active","active");
			values.put("id",this.id);
			MusicHandle.insertIntoTableEventual(coreName, "ActiveDetails", values);
		}		
		return isLockHolder;
	}
	
	//the main startup function for each daemon
	private void startHAFlow(){
		if (!Boolean.parseBoolean(ConfigReader.getConfigAttribute("start-as-active-"+id, "true"))) {
			//wait for active to start
			System.out.println("Checking to see if active has started");
			Map<String, Object> active = getActiveDetails();
			while (active==null || active.isEmpty()) {
				//spin
				System.out.println("Looking to start in passive mode. Waiting for active to start");
			}
		}
		
		while (true) {
			if (isActiveLockHolder()) {
				activeFlow();
			}
			else {
				passiveFlow();
			}
		}
	}
	
	private ScriptResult tryToEnsureCoreFunctioning(String id,CoreState mode, int noOfAttempts){
		ArrayList<String> script =null;
		ScriptResult result = ScriptResult.FAIL_RESTART;

		if (mode.equals(CoreState.ACTIVE)) {
			script = ConfigReader.getExeCommandWithParams("ensure-active-"+id);
		} else if (mode.equals(CoreState.PASSIVE)) {
			script = ConfigReader.getExeCommandWithParams("ensure-passive-"+id);
		}
		
		while (noOfAttempts > 0) {			
			result = HalUtil.executeBashScriptWithParams(script);
			if (result == ScriptResult.ALREADY_RUNNING) {
				System.out.println("Executed core script, the core was already running");
				return result;
			} else if (result == ScriptResult.SUCCESS_RESTART) {
				//we can now handle being after, put yourself back in queue
				lockRef = MusicHandle.createLockRef(lockName);
				System.out.println("Executed core script, the core had to be restarted");
				return result;
			} else if(result == ScriptResult.FAIL_RESTART) {
				noOfAttempts--;
				System.out.println("Executed core script, the core could not be re-started, retry attempts left ="+noOfAttempts);
			}
			//backoff period in between restart attempts
			try {
				Thread.sleep(Long.parseLong(ConfigReader.getConfigAttribute("restart-backoff-time", "0")));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return result;
	}

	private void updateHealth(String replicaId, CoreState mode){
		//create entry in passive table
		Map<String,Object> values = new HashMap<String,Object>();
		values.put("id",replicaId);
		if(mode.equals(CoreState.ACTIVE))
			values.put("isActive",true);	
		else
			values.put("isActive",false);		
		values.put("TimeOfLastUpdate", System.currentTimeMillis());
		values.put("lockRef", this.lockRef);
		MusicHandle.insertIntoTableEventual(coreName, "Replicas", values);
	}
	
	private boolean isReplicaAlive(String id){	
		Map<String,Object> valueMap = MusicHandle.readSpecificRow(coreName, "Replicas", "id", id);
		System.out.println("Checking health of hal-d "+id+"...");
		if (valueMap == null) {
			System.out.println("No entry showing...");
			return false; 
		}
		
		if (!valueMap.containsKey("timeoflastupdate")) {
			System.out.println("No 'timeoflastupdate' entry showing...");
			return false;
		}

		long lastUpdate = (Long)valueMap.get("timeoflastupdate");
		System.out.println("time of last update:"+lastUpdate);
	    long timeOutPeriod = Long.parseLong(ConfigReader.getConfigAttribute("hal-timeout"));
	    long currentTime = System.currentTimeMillis();
		System.out.println("current time:"+currentTime);
	    long timeSinceUpdate = currentTime-lastUpdate; 
		System.out.println("time since update:"+timeSinceUpdate);
	    if(timeSinceUpdate > timeOutPeriod)
	    	return false;
	    else
	    	return true;
	}
	
	private Map<String, Object> getActiveDetails(){
		Map<String,Object> results = MusicHandle.readAllRows(coreName, "ActiveDetails");
		for (Map.Entry<String, Object> entry : results.entrySet()){
			Map<String, Object> valueMap = (Map<String, Object>)entry.getValue();
			return valueMap;
		}
		return null;
	}

	private void releaseLock(String replicaId){
		String lockRef = getLockRef(replicaId);
		
		if(lockRef == null){
			System.out.println("There is no lock entry..");
			return;
		}
			
		if(lockRef.equals("")){
			System.out.println("Already unlocked..");
			return;
		}

		System.out.println("Unlocking hal "+replicaId + " with lockref"+ lockRef);
		MusicHandle.unlock(lockRef);
		System.out.println("Unlocked hal "+replicaId);
		
		//create entry in replicas table
		Map<String,Object> values = new HashMap<String,Object>();
//		values.put("id",replicaId);
		values.put("isActive",false);		
//		values.put("TimeOfLastUpdate", System.currentTimeMillis());
		values.put("lockRef", "");
		MusicHandle.updateTableEventual(coreName, "Replicas", "id", replicaId, values);
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
					
					System.out.println(lockRef + " status: "+MusicHandle.acquireLock(lockRef));
				}
			}
		}	
	}
	
	private boolean restartHALDaemon(String replicaId, int noOfAttempts){
		System.out.println("***Hal Daemon--"+replicaId+"---needs to be restarted***");

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
	
	private void takeOverFromCurrentActive(String currentActiveId){

		//try to restart the corresponding HAL daemon so that it starts the core in passive mode
		//restartHALDaemon(currentActiveId,3);
		
		boolean oldIdStillActive = true;
		long startTime = System.currentTimeMillis();
		long restartTimeout = Long.parseLong(ConfigReader.getConfigAttribute("hal-timeout"));
		while(true){
			Map<String,Object> replicaDetails = MusicHandle.readSpecificRow(coreName, "Replicas", "id", currentActiveId);
			oldIdStillActive  = (Boolean)replicaDetails.get("isactive");
			if(oldIdStillActive == false)
				break;
			
			//waited long enough..just make the old active passive yourself
			if((System.currentTimeMillis() - startTime) > restartTimeout){
				System.out.println("***Old Active not responding..resetting Music state of old active to passive myself***");
				updateHealth(currentActiveId, CoreState.PASSIVE);
				break;
			}
		}

		System.out.println("***Old Active has now become passive, so starting active flow ***");

		//now you can take over as active! 
		//activeFlow();
	}
	
	
	private void activeFlow(){
		
		while (true) {
			updateHealth(id, CoreState.ACTIVE);
			if(MusicHandle.acquireLock(lockRef) == false){
				System.out.println("******I no longer have the lock!Make myself passive*******");
				lockRef = MusicHandle.createLockRef(lockName);//put yourself back in the queue
				return;
			}
			int noOfAttempts = Integer.parseInt(ConfigReader.getConfigAttribute("noOfRetryAttempts"));
			ScriptResult result = tryToEnsureCoreFunctioning(id, CoreState.ACTIVE,noOfAttempts);

			if(result == ScriptResult.FAIL_RESTART){//unable to start core, just give up and become passive
				System.out.println("Tried enough times and still unable to start the core, giving up lock and starting passive flow..");
				MusicHandle.unlock(lockRef);
				return;
			}
			
			System.out.println("--(Active) Hal Daemon--"+id+"---CORE ACTIVE---Lock Ref:"+lockRef);
			
			System.out.println("--(Active) Hal Daemon--"+id+"---HEALTH  UPDATED---");
			System.out.println(lockRef + " status: "+MusicHandle.acquireLock(lockRef));
			tryToEnsurePeerHealth();
			System.out.println(lockRef + " status: "+MusicHandle.acquireLock(lockRef));
			System.out.println("--(Active) Hal Daemon--"+id+"---PEERS CHECKED---");

			//back off if needed
			try {
				Long sleeptime = Long.parseLong(ConfigReader.getConfigAttribute("core-monitor-sleep-time", "0"));
				if (sleeptime>0) {
					System.out.println("Sleeping for " + sleeptime + " seconds");
					Thread.sleep(sleeptime);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	private void passiveFlow(){
		while(true){
			//update own health in music
			updateHealth(this.id, CoreState.PASSIVE);
			System.out.println("--{Passive} Hal Daemon--"+id+"---HEALTH  UPDATED---");
	
			int noOfAttempts = Integer.parseInt(ConfigReader.getConfigAttribute("noOfRetryAttempts"));
			tryToEnsureCoreFunctioning(id, CoreState.PASSIVE,noOfAttempts);
			System.out.println("-- {Passive} Hal Daemon--"+id+"---CORE PASSIVE---Lock Ref:"+lockRef);


			//check if active is alive
			Map<String, Object> activeDetails =  getActiveDetails();
			//obtain active lock id
			String activeId = (String)activeDetails.get("id");
			
			Boolean isAlive = isReplicaAlive(activeId);
			if(isAlive == false || isActiveLockHolder()){
				System.out.println("*** ACTIVE "+"("+ activeId+") *** SUSPECTED DEAD!!");
				releaseLock(activeId);
				if (isActiveLockHolder()) {
					System.out.println("***I am the next in line, so taking over from active***");
					takeOverFromCurrentActive(activeId);
					return;
				}
			}
			System.out.println("--{Passive} Hal Daemon--"+id+"---ACTIVE  ALIVE---");

			//back off if needed
			try {
				Long sleeptime = Long.parseLong(ConfigReader.getConfigAttribute("core-monitor-sleep-time", "0"));
				if (sleeptime>0) {
					System.out.println("Sleeping for " + sleeptime + "seconds");
					Thread.sleep(sleeptime);
				}
			} catch (Exception e) {
				e.printStackTrace();
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
		String id = args[0];
		ConfigReader.setConfigLocation(args[1]);
		System.out.println("--Hal Daemon version "+HalUtil.version+"--replica id "+id+"---START---");
		HADaemon hd = new HADaemon(id);
		hd.startHAFlow();
	}

}
