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

import musicinterface.RestMusicFunctions;

public class HADaemon {
	String id;
	String lockName,lockRef;
	public static enum CoreState {PASSIVE, ACTIVE}
	String appName;
	public HADaemon(String id){
		this.id = id; 
		bootStrap();
	}
	
	private void bootStrap(){
		appName = ConfigReader.getConfigAttribute("appName");
		String keyspaceName=appName;
		RestMusicFunctions.createKeyspaceEventual(keyspaceName);
		
		String tableName = "Replicas";
		Map<String,String> replicaFields = new HashMap<String,String>();
		replicaFields.put("id", "text");
		replicaFields.put("isActive", "boolean");
		replicaFields.put("TimeOfLastUpdate", "varint");
		replicaFields.put("lockRef", "text");
		replicaFields.put("PRIMARY KEY", "(id)");

		RestMusicFunctions.createTableEventual(keyspaceName, tableName, replicaFields);	
		
		String activeTableName = "ActiveDetails";
		Map<String,String> activeTableFields = new HashMap<String,String>();
		activeTableFields.put("active", "text");
		activeTableFields.put("id", "text");
		activeTableFields.put("PRIMARY KEY", "(active)");
		RestMusicFunctions.createTableEventual(keyspaceName, activeTableName, activeTableFields);	
		
		
		lockName = ConfigReader.getConfigAttribute("appName")+".active";
		createLockRefIfDoesNotExist();

	}
	
	private void createLockRefIfDoesNotExist(){
		String oldLockRef = getLockRef(this.id);
		if((oldLockRef == null) || (oldLockRef.equals(""))){
			System.out.println("Lock does not exist already");
			lockRef = RestMusicFunctions.createLockRef(lockName);
			updateHealth(this.id, CoreState.PASSIVE);
		}
		else{
			System.out.println("Lock already exists");
			lockRef = oldLockRef;
		}
		
	}
	
	private String getLockRef(String replicaId){	
		//first check if a lock reference exists for this id..
		Map<String,Object> replicaDetails = RestMusicFunctions.readSpecificRow(appName, "Replicas", "id", replicaId); 
		if(replicaDetails == null){
			System.out.println("no enry found for this replica...");
			return null; 
		}
		System.out.println("Entry found:"+replicaDetails.get("lockref"));
		return (String)replicaDetails.get("lockref");
	}
	
	//this function maintains the key invariant that it will return true for only one id
	private boolean isActiveLockHolder(){
		boolean isLockHolder = RestMusicFunctions.acquireLock(lockRef);
		if(isLockHolder){//update active table
			Map<String,Object> values = new HashMap<String,Object>();
			values.put("active","active");
			values.put("id",this.id);
			RestMusicFunctions.insertIntoTableEventual(appName, "ActiveDetails", values);
		}		
		return isLockHolder;
	}
	
	//the main startup function for each daemon
	private void startHAFlow(){	
		if(isActiveLockHolder())
			activeFlow();
		else
			passiveFlow();
	}
	
	
	private boolean tryToEnsureCoreFunctioning(String id,CoreState mode, int noOfAttempts){
		ArrayList<String> script =null;
		boolean result = false;

		if(mode.equals(CoreState.ACTIVE))	
			script = ConfigReader.getExeCommandWithParams("ensure-active-"+id);
		else if(mode.equals(CoreState.PASSIVE))
			script = ConfigReader.getExeCommandWithParams("ensure-passive-"+id);
		
		while(result == false){
			result = HalUtil.executeBashScriptWithParams(script);
			noOfAttempts--;
			if(noOfAttempts <= 0)
				break; 
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
		RestMusicFunctions.insertIntoTableEventual(appName, "Replicas", values);
	}
	
	private boolean isReplicaAlive(String id){	
		Map<String,Object> valueMap = RestMusicFunctions.readSpecificRow(appName, "Replicas", "id", id);
		System.out.println("Checking health of hal-d "+id+"...");
		if(valueMap == null){
			System.out.println("No entry showing...");
			return false; 
		}

		long lastUpdate = (Long)valueMap.get("timeoflastupdate");
		System.out.println("time of last update:"+lastUpdate);
	    long timeOutPeriod = Long.parseLong(ConfigReader.getConfigAttribute("timeout"));
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
		Map<String,Object> results = RestMusicFunctions.readAllRows(appName, "ActiveDetails");
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
		RestMusicFunctions.unlock(lockRef);
		System.out.println("Unlocked hal "+replicaId);
		
		//create entry in replicas table
		Map<String,Object> values = new HashMap<String,Object>();
//		values.put("id",replicaId);
		values.put("isActive",false);		
//		values.put("TimeOfLastUpdate", System.currentTimeMillis());
		values.put("lockRef", "");
		RestMusicFunctions.updateTableEventual(appName, "Replicas", "id", replicaId, values);
	}
	
	private void tryToEnsurePeerHealth(){
		ArrayList<String> replicaList =  ConfigReader.getConfigListAttribute(("replicaIdList"));
		for (Iterator<String> iterator = replicaList.iterator(); iterator.hasNext();) {
			String replicaId = (String) iterator.next();
			if(replicaId.equals(this.id) == false){
				if(isReplicaAlive(replicaId) == false){
					//restart if suspected dead
					//releaseLock(replicaId);
					restartHALDaemon(replicaId, 2);
					System.out.println(lockRef + " status: "+RestMusicFunctions.acquireLock(lockRef));
				}
			}
		}	
	}
	
	private boolean restartHALDaemon(String replicaId, int noOfAttempts){
		System.out.println("***Hal Daemon--"+replicaId+"---needs to be restarted***");

		ArrayList<String> restartScript = ConfigReader.getExeCommandWithParams("restart-hal-"+replicaId);
		HalUtil.executeBashScriptWithParams(restartScript);
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
		restartHALDaemon(currentActiveId,3);
		
		boolean oldIdStillActive = true;
		long startTime = System.currentTimeMillis();
		long restartTimeout = Long.parseLong(ConfigReader.getConfigAttribute("timeout"));
		while(true){
			Map<String,Object> replicaDetails = RestMusicFunctions.readSpecificRow(appName, "Replicas", "id", currentActiveId);
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
		activeFlow();
	}
	
	
	private void activeFlow(){
		
		while(true){
			updateHealth(id, CoreState.ACTIVE);
			if(RestMusicFunctions.acquireLock(lockRef) == false){
				System.out.println("******I no longer have the lock!Make myself passive*******");
				lockRef = RestMusicFunctions.createLockRef(lockName);//put yourself back in the queue
				passiveFlow();
			}

			//update music with lockid
			
			int noOfAttempts = Integer.parseInt(ConfigReader.getConfigAttribute("noOfRetryAttempts"));
			boolean result = tryToEnsureCoreFunctioning(id, CoreState.ACTIVE,noOfAttempts);
			System.out.println("--(Active) Hal Daemon--"+id+"---CORE ACTIVE---Lock Ref:"+lockRef);

			if(result == false){//unable to start core, just give up and become passive
				System.out.println("Unable to start the core...giving up lock");
				RestMusicFunctions.unlock(lockRef);
				lockRef = RestMusicFunctions.createLockRef(lockName);//put yourself back in the queue
				passiveFlow();
			}
			
			System.out.println("--(Active) Hal Daemon--"+id+"---HEALTH  UPDATED---");
			System.out.println(lockRef + " status: "+RestMusicFunctions.acquireLock(lockRef));
			tryToEnsurePeerHealth();
			System.out.println(lockRef + " status: "+RestMusicFunctions.acquireLock(lockRef));
			System.out.println("--(Active) Hal Daemon--"+id+"---PEERS CHECKED---");

		}
	}
	
	private void passiveFlow(){
		while(true){
			//ensure you still have a lock
		//	createLockRefIfDoesNotExist();
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
			if(isAlive == false){
				System.out.println("*** ACTIVE "+"("+ activeId+") *** SUSPECTED DEAD!!");
				releaseLock(activeId);
				if(isActiveLockHolder()){
					System.out.println("***I am the next in line, so taking over from active***");
					takeOverFromCurrentActive(activeId);
				}
			}
			System.out.println("--{Passive} Hal Daemon--"+id+"---ACTIVE  ALIVE---");

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
