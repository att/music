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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

public class MusicConnector {
	
	//change this to point to relevant cluster
	public String[] musicNodes;

	public MusicConnector(String[] musicNodes){
		this.musicNodes = musicNodes; 
	}
	
	private String getMusicNodeIp(){
		Random r = new Random();
		int index = r.nextInt(musicNodes.length);	
		return musicNodes[index];
	}
	
	public String getMusicNodeURL(){
			String musicurl = "http://"+getMusicNodeIp()+":8080/MUSIC/rest/v2";
			return musicurl;
	}
	
	public boolean isHostUp(String serverAddress) { 
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
	
	/*	
	private static String getMusicNodeIp(){
		
		//return "54.224.168.13";
		return bigSiteMusicNode;
		String serverAddress;
		serverAddress = agaveMusicNode;
		while(isHostUp(serverAddress) != true)
			serverAddress = toggle(serverAddress);
		return serverAddress;
	}*/
}
