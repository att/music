package com.att.research.music.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonUpdate;
import com.att.research.music.main.MusicCore;
import com.att.research.music.main.MusicUtil;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

/*
 *  These are functions created purely for benchmarking purposes. 
 * 
 */
@Path("/benchmarks/")
public class RestMusicBmAPI {
	final static Logger logger = Logger.getLogger(RestMusicBmAPI.class);

	//pure zk calls...
	@POST
	@Path("/purezk/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkCreate(@PathParam("name") String nodeName) throws Exception{
		MusicCore.pureZkCreate("/"+nodeName);
	}

	@PUT
	@Path("/purezk/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkUpdate(JsonInsert insObj,@PathParam("name") String nodeName) throws Exception{
		logger.info("--------------Zk normal update-------------------------");
		long start = System.currentTimeMillis();
		MusicCore.pureZkWrite(nodeName, insObj.serialize());
		long end = System.currentTimeMillis();
		logger.info("Total time taken for Zk normal update:"+(end-start)+" ms");
	}

	@GET
	@Path("/purezk/{name}")
	@Consumes(MediaType.TEXT_PLAIN)
	public byte[] pureZkGet(@PathParam("name") String nodeName) throws Exception{
		return MusicCore.pureZkRead(nodeName);
	}
	
	@PUT
	@Path("/purezk/atomic/{lockname}/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkAtomicPut(JsonInsert insObj,@PathParam("lockname") String lockName,@PathParam("name") String nodeName) throws Exception{
		logger.info("--------------Zk atomic update-------------------------");
		long start = System.currentTimeMillis();
		String lockId = MusicCore.createLockReference(lockName);
		long leasePeriod = MusicUtil.defaultLockLeasePeriod;
		if(MusicCore.acquireLockWithLease(lockName, lockId, leasePeriod) == true){
			logger.info("acquired lock with id "+lockId);
			MusicCore.pureZkWrite(nodeName, insObj.serialize());
			boolean voluntaryRelease = true; 
			MusicCore.releaseLock(lockId,voluntaryRelease);
		}
		long end = System.currentTimeMillis();
		logger.info("Total time taken for Zk atomic update:"+(end-start)+" ms");
	}

	@GET
	@Path("/purezk/atomic/{lockname}/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkAtomicGet(JsonInsert insObj,@PathParam("lockname") String lockName,@PathParam("name") String nodeName) throws Exception{
		logger.info("--------------Zk atomic read-------------------------");
		long start = System.currentTimeMillis();
		String lockId = MusicCore.createLockReference(lockName);
		long leasePeriod = MusicUtil.defaultLockLeasePeriod;
		if(MusicCore.acquireLockWithLease(lockName, lockId, leasePeriod) == true){
			logger.info("acquired lock with id "+lockId);
			MusicCore.pureZkRead(nodeName);
			boolean voluntaryRelease = true; 
			MusicCore.releaseLock(lockId,voluntaryRelease);
		}
		long end = System.currentTimeMillis();
		logger.info("Total time taken for Zk atomic read:"+(end-start)+" ms");
	}

}
