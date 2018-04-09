package com.att.research.music.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
import com.att.research.music.main.ResultType;
import com.att.research.music.main.WriteReturnType;
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
	@Path("/purezk/atomic/{lockname}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkAtomicPut(JsonInsert updateObj,@PathParam("lockname") String lockname) throws Exception{
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		String consistency = updateObj.getConsistencyInfo().get("type");

		logger.info("--------------Zookeeper "+consistency+" update-"+operationId+"-------------------------");
		
		byte[] data = updateObj.serialize();
		long jsonParseCompletionTime = System.currentTimeMillis();

		String lockId = MusicCore.createLockReference(lockname);

		long lockCreationTime = System.currentTimeMillis();

		WriteReturnType lockAcqResult = MusicCore.acquireLock(lockname, lockId);
		long lockAcqTime = System.currentTimeMillis();
		long zkPutTime=0,lockReleaseTime=0;
		try {
			if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
				for(int i=0; i < updateObj.getBatchSize();++i)
					MusicCore.pureZkWrite(lockname, data);
				zkPutTime = System.currentTimeMillis();
				if(consistency.equals("atomic"))
					MusicCore.voluntaryReleaseLock(lockId);
				else 
				if(consistency.equals("atomic_delete_lock"))
					MusicCore.deleteLock(lockname);
				lockReleaseTime = System.currentTimeMillis();
			}else{
				MusicCore.destroyLockRef(lockId);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		long actualUpdateCompletionTime = System.currentTimeMillis();

	
		long endTime = System.currentTimeMillis();

		String lockingInfo = "|lock creation time:"+(lockCreationTime-jsonParseCompletionTime)+"|lock accquire time:"+(lockAcqTime-lockCreationTime)+
				"|zk put time:"+(zkPutTime-lockAcqTime);
		
		if(consistency.equals("atomic"))
			lockingInfo = lockingInfo+	"|lock release time:"+(lockReleaseTime-zkPutTime)+"|";
		else 
		if(consistency.equals("atomic_delete_lock"))
			lockingInfo = lockingInfo+	"|lock delete time:"+(lockReleaseTime-zkPutTime)+"|";

		String timingString = "Time taken in ms for Zookeeper "+consistency+" update-"+operationId+":"+"|total operation time:"+
				(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|update time:"+(actualUpdateCompletionTime-jsonParseCompletionTime)+lockingInfo;

		logger.info(timingString);
	}

	@GET
	@Path("/purezk/atomic/{lockname}/{name}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void pureZkAtomicGet(JsonInsert insObj,@PathParam("lockname") String lockName,@PathParam("name") String nodeName) throws Exception{
		logger.info("--------------Zk atomic read-------------------------");
		long start = System.currentTimeMillis();
		String lockId = MusicCore.createLockReference(lockName);
		WriteReturnType lockAcqResult = MusicCore.acquireLock(lockName, lockId);
		if(lockAcqResult.getResult().equals(ResultType.SUCCESS)){
			logger.info("acquired lock with id "+lockId);
			MusicCore.pureZkRead(nodeName);
			MusicCore.voluntaryReleaseLock(lockId);
		}else{
			MusicCore.destroyLockRef(lockId);
		}

		long end = System.currentTimeMillis();
		logger.info("Total time taken for Zk atomic read:"+(end-start)+" ms");
	}

	//doing an update directly to cassa but through the rest api
	@PUT
	@Path("/cassa/keyspaces/{keyspace}/tables/{tablename}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public boolean updateTableCassa(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info	) throws Exception{
		long startTime = System.currentTimeMillis();
		String operationId = UUID.randomUUID().toString();//just for debugging purposes. 
		String consistency = insObj.getConsistencyInfo().get("type");
		logger.info("--------------Cassandra "+consistency+" update-"+operationId+"-------------------------");
		Map<String,Object> valuesMap =  insObj.getValues();
		TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		String vectorTs = "'"+Thread.currentThread().getId()+System.currentTimeMillis()+"'";
		String fieldValueString="vector_ts="+vectorTs+",";
		int counter =0;
		for (Map.Entry<String, Object> entry : valuesMap.entrySet()){
			Object valueObj = entry.getValue();	
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String valueString = MusicCore.convertToCQLDataType(colType,valueObj);	
			fieldValueString = fieldValueString+ entry.getKey()+"="+valueString;
			if(counter!=valuesMap.size()-1)
				fieldValueString = fieldValueString+",";
			counter = counter +1;
		}

		//get the row specifier
		String rowSpec="";
		counter =0;
		String query =  "UPDATE "+keyspace+"."+tablename+" ";   
		MultivaluedMap<String, String> rowParams = info.getQueryParameters();
		String primaryKey = "";
		for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()){
			String keyName = entry.getKey();
			List<String> valueList = entry.getValue();
			String indValue = valueList.get(0);
			DataType colType = tableInfo.getColumn(entry.getKey()).getType();
			String formattedValue = MusicCore.convertToCQLDataType(colType,indValue);	
			primaryKey = primaryKey + indValue;
			rowSpec = rowSpec + keyName +"="+ formattedValue;
			if(counter!=rowParams.size()-1)
				rowSpec = rowSpec+" AND ";
			counter = counter +1;
		}


		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();

		if((ttl != null) && (timestamp != null)){
			query = query + " USING TTL "+ ttl +" AND TIMESTAMP "+ timestamp;
		}

		if((ttl != null) && (timestamp == null)){
			query = query + " USING TTL "+ ttl;
		}

		if((ttl == null) && (timestamp != null)){
			query = query + " USING TIMESTAMP "+ timestamp;
		}
		query = query + " SET "+fieldValueString+" WHERE "+rowSpec+";";
		
		long jsonParseCompletionTime = System.currentTimeMillis();

		boolean operationResult = true;	
		MusicCore.getDSHandle().executePut(query, insObj.getConsistencyInfo().get("type"));
		
		long actualUpdateCompletionTime = System.currentTimeMillis();

		long endTime = System.currentTimeMillis();
		
		String timingString = "Time taken in ms for Cassandra "+consistency+" update-"+operationId+":"+"|total operation time:"+
				(endTime-startTime)+"|json parsing time:"+(jsonParseCompletionTime-startTime)+"|update time:"+(actualUpdateCompletionTime-jsonParseCompletionTime)+"|";
		logger.info(timingString);

		return operationResult; 	
	}


}
