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
package com.att.research.music.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Scanner;

public class MusicUtil {
	public static String myZkHost = "localhost";
	public static String myCassaHost = "localhost";
	public static final String musicInternalKeySpaceName = "MusicInternalKeySpace";
	public static final String nodeIdsTable ="nodeIds";
	public static final String evPutsTable = "evPutTracker_";
	public static final boolean debug = true;
	public static final String version = "1.0.0";
	public static final String msg = "-First version of Music on the public git-";
	
/*	public static String getMyPublicIp(){
		String myIp = null;
		try {
			Scanner fileScanner = new Scanner(new File(confLocation));
			fileScanner.next();//ignore id line
			String line = fileScanner.next();
			String[] idArray = line.split(":");
			myIp = idArray[1];
			fileScanner.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return myIp;	
		String agaveMusicNode = "135.207.223.43";
		String bigSiteMusicNode = "135.197.226.98";
		if((System.currentTimeMillis() %2)==0)
			return agaveMusicNode;
		else 
			return bigSiteMusicNode;
	}
	
	public static String getMusicNodeURL(){
		return "http://"+MusicUtil.getMyPublicIp()+":8080/MUSIC/rest";
	}
*/	
	public static String getMyId(){
		InetAddress IP;
		String hostName="";
		try {
			IP = Inet4Address.getLocalHost();
			hostName = IP.getHostName();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return hostName.replace("-", "_");

	}
	

/*	public static ArrayList<String> getOtherMusicPublicIps(){
		ArrayList<String> listOfNodeIps = new ArrayList<String>();
		try {
			Scanner fileScanner = new Scanner(new File(confLocation));
			fileScanner.next();//ignore the id line
			fileScanner.next();//ignore the my public ip line			
			fileScanner.next();//ignore the node ids line

			String nodeIpsline = fileScanner.next();
			String myIp = getMyPublicIp();
			String[] nodeIps = nodeIpsline.split(":");
			for(int i=1; i < nodeIps.length;i++){
				String otherIp = nodeIps[i];
				if(otherIp.equals(myIp) == false)
					listOfNodeIps.add(otherIp);
			}
			fileScanner.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return listOfNodeIps;
	}
	
	public static ArrayList<String> getOtherMusicNodeIds(){
		ArrayList<String> listOfNodeIds = new ArrayList<String>();
		try {
			Scanner fileScanner = new Scanner(new File(confLocation));
			fileScanner.next();//ignore the my id line
			fileScanner.next();//ignore the my public ip line

			String nodeIpsline = fileScanner.next();
			String[] nodeIds = nodeIpsline.split(":");
			String myId = getMyId();
			for(int i=1; i < nodeIds.length;i++){
				String otherId = nodeIds[i];
				if(otherId.equals(myId)==false)
					listOfNodeIds.add(otherId);
			}
			fileScanner.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return listOfNodeIds;
	}
*/
	public static String getTestType(){
		String testType = "";
		try {
			Scanner fileScanner = new Scanner(new File(""));
			testType = fileScanner.next();//ignore the my id line
			String batchSize = fileScanner.next();//ignore the my public ip line
			fileScanner.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return testType;

	}
	public static void sleep(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public static void main(String[] args){
		System.out.println(MusicUtil.getMyId());
	}
	
}
