package com.att.research.music.benchmarks;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.main.MusicCoreNonStatic;
import com.datastax.driver.core.ResultSet;
 
public class MusicJmeterClient{
    private ArrayList<MusicCoreNonStatic> musicHandles;
    private ArrayList<String> lockNames;
    public enum OperationType {cassandraGet, cassandraQuorumGet, cassandraEventualPut, 
    	cassandraQuorumPut, zkPut, zkGet, musicEventualPut, musicEventualGet, musicCriticalPutBlock, musicCriticalGetBlock};
    public MusicJmeterClient(String[] musicNodes){
    	lockNames = new ArrayList<String>();
    	musicHandles = new ArrayList<MusicCoreNonStatic>();
    	for (String nodeIp : musicNodes)
    		musicHandles.add(new MusicCoreNonStatic(nodeIp));
    	
    	try {
			initialize();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }   
    private MusicCoreNonStatic getRandomMusicClient(){
    	Random rand = new Random();
		return musicHandles.get(rand.nextInt(musicHandles.size()));	
    }
    
    public void initialize() throws Exception{
    	//create key space
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	mc.createKeyspace("shankarks");
    	
		JsonTable jtab = new JsonTable();
		Map<String,String> fields = new HashMap<String,String>();
		fields.put("name", "text");
		fields.put("salary", "varint");
		fields.put("PRIMARY KEY", "(name)");
		Map<String,String> consistencyInfo= new HashMap<String, String>();
		consistencyInfo.put("type", "eventual");
		jtab.setFields(fields);
		jtab.setConsistencyInfo(consistencyInfo);

		mc.createTable("shankarks","employees",jtab);
		
		//insert some rows
		
		Map<String,Object> firstRow = new HashMap<String,Object>();
		firstRow.put("name", "bharath");
		firstRow.put("salary", 4000);
		JsonInsert jins = new JsonInsert();
		jins.setValues(firstRow);
		jins.setConsistencyInfo(consistencyInfo);
		mc.insertIntoTable("shankarks","employees",jins); 
		
		Map<String,Object> secondRow = new HashMap<String,Object>();
		secondRow.put("name", "shankar");
		secondRow.put("salary", 8000);
		jins.setValues(secondRow);
		jins.setConsistencyInfo(consistencyInfo);
		mc.insertIntoTable("shankarks","employees",jins); 

		
		mc.pureZkCreate("/shankarks");

    }
    
    private void insertRow(Map<String, Object> values){
    	MusicCoreNonStatic mc = getRandomMusicClient();

    }
    public void cleanUp() throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	mc.dropKeyspace("shankarks");
    	
    	for ( String lockName : lockNames)
			mc.deleteLock(lockName);	
    }
    
    public void cassandraGet() throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String query = "select * from shankarks.employees where name='bharath'";
    	ResultSet rs = mc.getDSHandle().executeEventualGet(query);
    	System.out.println(rs.one().getString("name"));
    }
    
    public void cassandraQuorumGet() throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String query = "select * from shankarks.employees where name='bharath'";
    	ResultSet rs = mc.getDSHandle().executeCriticalGet(query);
    	System.out.println(rs.one().getString("name"));
    }
      
    public void cassandraEventualPut() throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String query = "update shankarks.employees set salary=5000 where name='bharath'";
    	mc.getDSHandle().executePut(query, "eventual");
    }
    
    public void cassandraQuorumPut() throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String query = "update shankarks.employees set salary=6000 where name='bharath'";
    	mc.getDSHandle().executePut(query, "atomic");
    }
    
    public void zkPut(){
		Map<String,Object> value = new HashMap<String,Object>();
		value.put("name", "bharath");
		value.put("salary", 5000);
    	JsonInsert insObj = new JsonInsert();
    	insObj.setValues(value);
    	MusicCoreNonStatic mc = getRandomMusicClient();
		mc.pureZkWrite("shankarks", insObj.serialize());
    }
    
    public void zkGet(){
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	System.out.println(mc.pureZkRead("shankarks"));
    	
    }
    
    public void musicEventualPut() throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String query = "update shankarks.employees set salary=7000 where name='bharath'";
		mc.eventualPut("shankarks","employees", "bharath", query);
    }
    
    public void musicEventualGet() throws Exception{
    	cassandraGet();
    }
    
    public void musicCriticalPutBlock(int blockSize) throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String lockName = "shankarks.employees.bharath";
    	String lockId = mc.createLockReference(lockName);
    	String query = "update shankarks.employees set salary=8000 where name='bharath'";
    	while(mc.acquireLock(lockName,lockId));
    	for(int i=0; i < blockSize; ++i)
    		mc.criticalPut("shankarks","employees", "bharath", query,lockId);
    	mc.unLock(lockId);
    	mc.deleteLock(lockName);
    }

    public void musicCriticalGetBlock(int blockSize) throws Exception{
    	MusicCoreNonStatic mc = getRandomMusicClient();
    	String lockName = "shankarks.employees.bharath";
    	String lockId = mc.createLockReference(lockName);
    	String query = "update shankarks.employees set salary=9000 where name='bharath'";
    	while(mc.acquireLock(lockName,lockId));
    	for(int i=0; i < blockSize; ++i){
    		ResultSet rs = mc.criticalGet("shankarks","employees", "bharath", query,lockId);
        	System.out.println(rs.one().getString("name"));

    	}
    	mc.unLock(lockId);
    	mc.deleteLock(lockName);
    }
    
    public void runTests(OperationType type, int blockSize){
    	try{
	    	switch(type){
	    		case cassandraGet: 
	    			cassandraGet();break;
	    		case cassandraEventualPut:
	    			cassandraEventualPut();break;
	    		case cassandraQuorumPut:
	    			cassandraQuorumPut();break;
	    		case cassandraQuorumGet:
	    			cassandraQuorumGet();break; 
	    		case zkGet:
	    			zkGet();break;
	    		case zkPut:
	    			zkPut();break;
	    		case musicEventualGet:
	    			musicEventualGet();break;
	    		case musicEventualPut:
	    			musicEventualPut();break;
	    		case musicCriticalGetBlock:
	    			musicCriticalGetBlock(blockSize);
	    		case musicCriticalPutBlock:
	    			musicCriticalPutBlock(blockSize);    		
	    	}	
    	}catch(Exception e){
    		e.printStackTrace();
    	}
    }
    public static void main(String[] args){
    	String[] musicNodes = {"localhost"};
    	MusicJmeterClient mjClient = new MusicJmeterClient(musicNodes);
    	int blockSize =1;
    	for (OperationType type: MusicJmeterClient.OperationType.values()) {
    		mjClient.runTests(type, blockSize);	
		}
    }
}