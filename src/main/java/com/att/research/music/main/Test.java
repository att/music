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

public class Test {

	public static void main(String[] args){
		String lockId = "$teslklklkt$klklklkl";
		String a = lockId.substring(lockId.indexOf("$") + 1);
		a = a.substring(0, a.indexOf("$"));
		System.out.println("heloooo:"+a);
		String test = "a.b.c";
		String[] testSplit = test.split("\\.");
		System.out.println(testSplit[1]);
		
		lockId = "$testmusic$x-0000000000";
		String lockName = lockId.substring(lockId.indexOf("$")+1, lockId.lastIndexOf("$"));
		System.out.println("aila:"+ lockName);
		
		String key = "testmusic";
		String[] splitString = key.split("\\.");
		String keyspaceName = splitString[0];
		String tableName = splitString[1];
		String primaryKey = splitString[2];

		

	}
}
