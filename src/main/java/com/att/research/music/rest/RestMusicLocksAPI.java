package com.att.research.music.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.jsonobjects.JsonLeasedLock;
import com.att.research.music.lockingservice.MusicLockState;
import com.att.research.music.main.MusicCore;
import com.att.research.music.main.ResultType;
import com.att.research.music.main.ReturnType;


@Path("/locks/")
public class RestMusicLocksAPI {
	final static Logger logger = Logger.getLogger(RestMusicLocksAPI.class);
	/*	puts the requesting process in the q for this lock. The corresponding node will be
	created in zookeeper if it did not already exist*/
	@POST
	@Path("/create/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String createLockReference(@PathParam("lockname") String lockName){
		return MusicCore.createLockReference(lockName);
	}

	//checks if the node is in the top of the queue and hence acquires the lock
	@GET
	@Path("/acquire/{lockreference}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String accquireLock(@PathParam("lockreference") String lockId){
		String lockName = lockId.substring(lockId.indexOf("$")+1, lockId.lastIndexOf("$"));
		ReturnType result = MusicCore.acquireLock(lockName,lockId);
		if(result.getResultType().equals(ResultType.SUCCESS))
			return "true"; 
		else 
			return "false";
	}
	
	@POST
	@Path("/acquire-with-lease/{lockreference}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)	
	public String accquireLockWithLease(JsonLeasedLock lockObj, @PathParam("lockreference") String lockId){
		String lockName = lockId.substring(lockId.indexOf("$")+1, lockId.lastIndexOf("$"));
		//lockName is the "key" of the form keyspaceName.tableName.rowId
		ReturnType result = MusicCore.acquireLock(lockName,lockId);
		if(result.getResultType().equals(ResultType.SUCCESS))
			return "true"; 
		else 
			return "false";
	} 
	

	@GET
	@Path("/enquire/{lockname}")
	@Produces(MediaType.TEXT_PLAIN)	
	public String currentLockHolder(@PathParam("lockname") String lockName){
		return MusicCore.whoseTurnIsIt(lockName);
	}


	//deletes the process from the zk queue
	@DELETE
	@Path("/release/{lockreference}")
	public void unLock(@PathParam("lockreference") String lockId){
		MusicCore.voluntaryReleaseLock(lockId);
	}

	@DELETE
	@Path("/delete/{lockname}")
	public void deleteLock(@PathParam("lockname") String lockName){
		MusicCore.deleteLock(lockName);
	}

}
