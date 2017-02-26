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


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Map;

import musicinterface.RestMusicFunctions;



public class HalUtil {
	public static String zkHostAddress = "localhost";
	public static String cassaHostAddress = "localhost";
	//public static String bigSiteMusicNode = "localhost";
	//public static String agaveMusicNode = "localhost";
	
	private static String getMusicNodeIp(){
		return "localhost";
/*		String serverAddress;
		serverAddress = agaveMusicNode;
		while(isHostUp(serverAddress) != true)
			serverAddress = toggle(serverAddress);
		return serverAddress;
*/	}
	
/*	public static String toggle(String serverAddress){
		if(serverAddress.equals(agaveMusicNode)){
			System.out.println("Agave is down...connect to Big Site");
			serverAddress = bigSiteMusicNode;
		}else if(serverAddress.equals(bigSiteMusicNode)){
			System.out.println("Big Site is down...connect to Agave");
			serverAddress = agaveMusicNode;
		}
		return serverAddress;
	}*/
	
	public static String getMusicNodeURL(){
			return "http://"+HalUtil.getMusicNodeIp()+":8080/MUSIC/rest";
	}
	
	public static boolean isHostUp(String serverAddress) { 
		Boolean isUp = false;
	    try {
			InetAddress inet = InetAddress.getByName(serverAddress);
			isUp = inet.isReachable(1000);	
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    return isUp;
	}
	
	
	public static boolean executeBashScriptWithParams(ArrayList<String> script){
		try {
			ProcessBuilder pb = new ProcessBuilder(script);
			final Process process = pb.start();
			process.waitFor();
			InputStream is = process.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String result = br.readLine();
			if(result == null)
				return false;
			if(result.equals("Running"))
				return true;
			else return false;
/*			String s;
			ArrayList<String> opText = new ArrayList<String>();
			  while ((s = br.readLine()) != null) {
	                opText.add(s);
	            }
			  System.out.println(opText.size());
			  System.out.println(opText);

*/		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	private static Map<String, Object> getActiveDetails(){
		Map<String,Object> results = RestMusicFunctions.readAllRows("votingAppBharath", "ActiveDetails");
		for (Map.Entry<String, Object> entry : results.entrySet()){
			Map<String, Object> valueMap = (Map<String, Object>)entry.getValue();
			return valueMap;
		}
		return null;
	}

	public static void main(String[] args){
/*	//	String folderPath = "/Users/bharathb/AttWork/Music/Rest-Music/examples/VoteAppMusicJava/scripts/";
		String folderPath = "/Users/bharathb/AttWork/Music/Rest-Music/recipes/Hal/scripts/";
		ArrayList<String> script = new ArrayList<String>();
		script.add(folderPath+ "restartHalIfDead.sh");
		script.add("0");
	//	script.add("active");

		//System.out.println(VotingApp.executeBashScript(folderPath+ "ensureVotingAppRunning.sh", "2", "active"));
		
	//	System.out.println(HalUtil.executeBashScriptWithParams(script));
		Map<String, Object> activeMap = getActiveDetails();
		String lockref = (String)activeMap.get("lockref");
		System.out.println(lockref);
		
		Map<String,Object> replicaDetails = RestMusicFunctions.readSpecificRow("votingAppBharath", "Replicas", "id", "0");
		System.out.println(replicaDetails);
		boolean convertedToPassive = (Boolean)replicaDetails.get("isactive");
		System.out.println(convertedToPassive);
*/		
		
		ConfigReader.setConfigLocation("/Users/bharathb/AttWork/Music/Rest-Music/recipes/Hal/scripts");

		ArrayList<String> restartScript = ConfigReader.getExeCommandWithParams("test");
		HalUtil.executeBashScriptWithParams(restartScript);

	}
}
