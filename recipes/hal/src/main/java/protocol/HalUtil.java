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

import protocol.HADaemon.ScriptResult;
import musicinterface.MusicHandle;



public class HalUtil {
	public static String version="1.1.0";
	private static String getMusicNodeIp(){
		return ConfigReader.getConfigAttribute("musicLocation");
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
	
	
	public static ScriptResult executeBashScriptWithParams(ArrayList<String> script){
		try {
			ProcessBuilder pb = new ProcessBuilder(script);
			final Process process = pb.start();
			int exitCode = process.waitFor();

			StringBuffer errorOutput = new StringBuffer();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
            String line = "";                       
            while ((line = reader.readLine())!= null) {
            		if(!line.equals(""))
            			errorOutput.append(line + "\n");
            }
            System.out.print(errorOutput);
			if(exitCode == 0)
				return ScriptResult.ALREADY_RUNNING;
			else
			if(exitCode == 1)
				return ScriptResult.FAIL_RESTART;
			else
			if(exitCode == 2)
				return ScriptResult.SUCCESS_RESTART;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ScriptResult.FAIL_RESTART;
	}
	
	private static Map<String, Object> getActiveDetails(){
		Map<String,Object> results = MusicHandle.readAllRows("votingAppBharath", "ActiveDetails");
		for (Map.Entry<String, Object> entry : results.entrySet()){
			Map<String, Object> valueMap = (Map<String, Object>)entry.getValue();
			return valueMap;
		}
		return null;
	}
	
	public static void main(String[] args){
		ArrayList<String> script = new ArrayList<String>();
		script.add("/Users/bharathb/AttWork/Music/hal/halTesting/sampleAppFolder/ensureVotingAppRunning.sh");
		script.add("0");
		script.add("passive");
		System.out.println(HalUtil.executeBashScriptWithParams(script));
	}

}
