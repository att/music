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

public class MusicDigest {
	private String evPutStatus;
	private String vectorTs;
	public MusicDigest(String evPutStatus, String vectorTs){
		this.evPutStatus = evPutStatus;
		this.vectorTs = vectorTs;
	}
	public String getEvPutStatus() {
		return evPutStatus;
	}
	public void setEvPutStatus(String evPutStatus) {
		this.evPutStatus = evPutStatus;
	}
	public String getVectorTs() {
		return vectorTs;
	}
	public void setVectorTs(String vectorTs) {
		this.vectorTs = vectorTs;
	}
	public String toString(){
		return vectorTs + "|" + evPutStatus;
	}
	
}
