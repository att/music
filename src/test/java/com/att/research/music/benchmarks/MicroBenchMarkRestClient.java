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
package com.att.research.music.benchmarks;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.att.research.music.main.MusicUtil;


@Path("/tests")
public class MicroBenchMarkRestClient {
	static double version = 9.7;
	
	@GET
	@Path("/run")
	@Produces(MediaType.TEXT_PLAIN)
	public String runTests() {
		String testType = MusicUtil.getTestType();
		String candidateName = "shankar"+System.currentTimeMillis();
		MicroBenchMarks msHandle = new MicroBenchMarks();
		switch (testType) {
        case "musicPut":  return msHandle.musicPutAndUpdate(candidateName);
        
        case "musicCriticalPut":  return msHandle.musicCriticalPutAndUpdate(candidateName);
        
        case "musicGet":  return msHandle.musicGet();

        case "cassaPut":  return msHandle.cassaPutAndUpdate(candidateName);
        
        case "cassaQuorumPut":  return msHandle.cassaQuorumPutAndUpdate(candidateName);
        
        case "cassaGet":  return msHandle.cassaGet();

        case "zkPut":  return msHandle.zkPutAndUpdate(candidateName);

        case "zkGet":  return msHandle.zkGet();

		}
		return "something wrong!";
	
	}

}

