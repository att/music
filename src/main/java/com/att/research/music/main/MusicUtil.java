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
	public static String defaultMusicIp = "localhost";
	public static final String musicInternalKeySpaceName = "MusicInternalKeySpace";
	public static final String nodeIdsTable ="nodeIds";
	public static final String evPutsTable = "evPutTracker_";
	public static final boolean debug = true;
	public static final String version = "1.0.2";
	public static final String musicRestIp = "localhost";
	
	public static String getMyId(){
		InetAddress IP;
		String hostName="";
		try {
			IP = Inet4Address.getLocalHost();
			hostName = 	IP.getHostName();	
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		hostName = hostName.replace("-", "_");
		hostName = hostName.replace(".", "_");
		return hostName;
	}
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
}
