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
package main;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class HaVApp {
	VotingApp vHandle;
	String scriptPath;
	int id;
	private final String version="1.0.0";
	public HaVApp(int id, String scriptPath){
		this.id = id;
		this.scriptPath = scriptPath;
		String[] musicNode = {"localhost"};
		vHandle = new VotingApp(musicNode); 
		System.out.println("Started Voting App Replica"+id);
	}
	
	public void executeActiveFlow(){
		System.out.println("----------Voting app Replica "+id+" Executing as ACTIVE----------------");
		System.out.println(vHandle.readAllVotes());

/*		updateVoteCountAtomically("Popeye",r.nextInt(7));
		System.out.println(readVoteCountForCandidate("Popeye"));

		updateVoteCountAtomically("Judy",r.nextInt(7));
		System.out.println(readVoteCountForCandidate("Judy"));

		updateVoteCountAtomically("Mickey",r.nextInt(7));
		System.out.println(readVoteCountForCandidate("Mickey"));

		
		updateVoteCountAtomically("Flash",r.nextInt(7));
		System.out.println(readVoteCountForCandidate("Flash"));
*/	}
	private boolean isActive(){
		System.out.println("HA-Voting app version "+ version+" .....");
		try {
			Scanner fileScanner = new Scanner(new File(scriptPath+"/modeOfCoreReplica"+id+".out"));
			while(fileScanner.hasNext()){
				String mode = fileScanner.next();
				if(mode.equalsIgnoreCase("active"))
					return true; 
				else 
					return false; 
			}
			fileScanner.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	

	public void executePassiveFlow(){
		System.out.println("----------Voting app Replica "+id+" Executing as PASSIVE----------------");
		System.out.println(vHandle.readAllVotes());
	}
		
	public void HACoreTest(){
		while(true){
			if(isActive()){
				vHandle.deleteAllLocks();
				executeActiveFlow();
			}
			else
				executePassiveFlow();
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public static void main(String[] args){
		System.out.println("Core Voting app purely for HA tests..");
		int id =Integer.parseInt(args[0]); String scriptPath = args[1];
		HaVApp vHA = new HaVApp(id, scriptPath);
		vHA.HACoreTest();
	}

}
