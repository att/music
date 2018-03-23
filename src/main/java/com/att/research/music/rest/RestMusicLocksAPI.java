package com.att.research.music.rest;

import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.jsonobjects.JsonLeasedLock;
import com.att.research.music.datastore.jsonobjects.JsonLockResponse;
import com.att.research.music.lockingservice.MusicLockState;
import com.att.research.music.main.MusicCore;
import com.att.research.music.main.ResultType;
import com.att.research.music.main.WriteReturnType;


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
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String,Object> accquireLock(
            @PathParam("lockreference") String lockId,
            @Context HttpServletResponse response){
        String lockName = lockId.substring(lockId.indexOf('$')+1, lockId.lastIndexOf('$'));
        WriteReturnType lockStatus = MusicCore.acquireLock(lockName,lockId);
        return new JsonLockResponse(lockStatus.getResult()).setLock(lockId)
                                    .setMessage(lockStatus.getMessage()).toMap();
    }
	
	@POST
	@Path("/acquire-with-lease/{lockreference}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.TEXT_PLAIN)	
	public Map<String,Object> accquireLockWithLease(JsonLeasedLock lockObj, @PathParam("lockreference") String lockId){
		String lockName = lockId.substring(lockId.indexOf("$")+1, lockId.lastIndexOf("$"));
		//lockName is the "key" of the form keyspaceName.tableName.rowId
		WriteReturnType lockStatus = MusicCore.acquireLock(lockName,lockId);
		return new JsonLockResponse(lockStatus.getResult()).setLock(lockId)
                .setMessage(lockStatus.getMessage()).toMap();
	} 
	

	@GET
	@Path("/enquire/{lockname}")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, Object> currentLockHolder(@PathParam("lockname") String lockName){
		String who = MusicCore.whoseTurnIsIt(lockName);
		ResultType status = ResultType.SUCCESS;
		String error = "";
		if ( who == null ) { 
			status = ResultType.FAILURE; 
			error = "There was a problem getting the lock holder";
		}
		return new JsonLockResponse(status).setError(error)
						.setLock(lockName).setLockHolder(who).toMap();
		
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
