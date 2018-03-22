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
package com.att.research.music.datastore;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicDataStore {
	private Session session;
	private Cluster cluster;
	final static Logger logger = Logger.getLogger(MusicDataStore.class);

	public MusicDataStore(){
		connectToCassaCluster();
	}

	public MusicDataStore(String remoteIp){
		connectToCassaCluster(remoteIp);
	}

	private ArrayList<String> getAllPossibleLocalIps(){
		ArrayList<String> allPossibleIps = new ArrayList<String>();
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while(en.hasMoreElements()){
			    NetworkInterface ni=(NetworkInterface) en.nextElement();
			    Enumeration<InetAddress> ee = ni.getInetAddresses();
			    while(ee.hasMoreElements()) {
			        InetAddress ia= (InetAddress) ee.nextElement();
			        allPossibleIps.add(ia.getHostAddress());
			    }
			 }
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return allPossibleIps;
	}
	
	private void connectToCassaCluster(){
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		String address= "localhost";
		logger.debug("Connecting to cassa cluster: Iterating through possible ips:"+getAllPossibleLocalIps());
		while(it.hasNext()){
			try {
				cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
				//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
				Metadata metadata = cluster.getMetadata();
				logger.debug("Connected to cassa cluster "+metadata.getClusterName()+" at "+address);
/*				for ( Host host : metadata.getAllHosts() ) {
						.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
							host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
							
				}*/
				session = cluster.connect();
				
				break;
			} catch (NoHostAvailableException e) {
				address= it.next();
			} 
		}
	}
	
/*	public  ArrayList<String> getAllNodePublicIps(){
		Metadata metadata = cluster.getMetadata();
		ArrayList<String> nodePublicIps = new ArrayList<String>();
		for ( Host host : metadata.getAllHosts() ) {
			nodePublicIps.add(host.getBroadcastAddress().getHostAddress());
		}
		return nodePublicIps;
	}
*/	
	private void connectToCassaCluster(String address){	
		cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
		Metadata metadata = cluster.getMetadata();
		logger.debug("Connected to cassa cluster "+metadata.getClusterName()+" at "+address);
/*		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
					host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
		}*/
		session = cluster.connect();
	}

	public ResultSet executeEventualGet(String query){
		logger.info("Executing normal get query:"+query);
		long start = System.currentTimeMillis();
		Statement statement = new SimpleStatement(query);
		statement.setConsistencyLevel(ConsistencyLevel.ONE);
		ResultSet results = session.execute(statement);
		long end = System.currentTimeMillis();
		logger.debug("Time taken for actual get in cassandra:"+(end-start));
		return results;	
	}

	public ResultSet executeCriticalGet(String query){
		Statement statement = new SimpleStatement(query);
		logger.info("Executing critical get query:"+query);
		statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		ResultSet results = session.execute(statement);
		return results;	
	}

	public DataType returnColumnDataType(String keyspace, String tableName, String columnName){
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(keyspace);
		TableMetadata table = ks.getTable(tableName);
		return table.getColumn(columnName).getType();

	}

	public TableMetadata returnColumnMetadata(String keyspace, String tableName){
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(keyspace);
		return ks.getTable(tableName);
	}

	public void executePut(String query, String consistency){
		logger.debug("in data store handle, executing put:"+query);
		long start = System.currentTimeMillis();
		Statement statement = new SimpleStatement(query);
		if(consistency.equalsIgnoreCase("critical")){
			//logger.info("Executing critical put query:"+query);
			statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		}
		else if (consistency.equalsIgnoreCase("eventual")){
			logger.info("Executing normal put query:"+query);
			statement.setConsistencyLevel(ConsistencyLevel.ONE);
		}
		session.execute(statement); 
		long end = System.currentTimeMillis();
		logger.debug("Time taken for actual put in cassandra:"+(end-start));
	}

	public Object getColValue(Row row, String colName, DataType colType){	
		switch(colType.getName()){
		case VARCHAR: 
			return row.getString(colName);
		case UUID: 
			return row.getUUID(colName);
		case VARINT: 
			return row.getVarint(colName);
		case BIGINT: 
			return row.getLong(colName);
		case INT: 
			return row.getInt(colName);
		case FLOAT: 
			return row.getFloat(colName);	
		case DOUBLE: 
			return row.getDouble(colName);
		case BOOLEAN: 
			return row.getBool(colName);
		case MAP: 
			return row.getMap(colName, String.class, String.class);
		default: 
			return null;
		}
	}
	
	public boolean doesRowSatisfyCondition(Row row, Map<String, Object> condition){
		ColumnDefinitions colInfo = row.getColumnDefinitions();
		
		for (Map.Entry<String, Object> entry : condition.entrySet()){
			String colName = entry.getKey();
			DataType colType = colInfo.getType(colName);
			Object columnValue = getColValue(row, colName, colType);
			Object conditionValue = convertToActualDataType(colType, entry.getValue());
			if(columnValue.equals(conditionValue) == false)
				return false;		
		}
		return true;	
	}

	public Map<String, HashMap<String, Object>> marshalData(ResultSet results){
		Map<String, HashMap<String, Object>> resultMap = new HashMap<String, HashMap<String,Object>>();
		int counter =0;
		for (Row row : results) {
			ColumnDefinitions colInfo = row.getColumnDefinitions();
			HashMap<String,Object> resultOutput = new HashMap<String, Object>();
			for (Definition definition : colInfo) {
				if(!definition.getName().equals("vector_ts"))
					resultOutput.put(definition.getName(), getColValue(row, definition.getName(), definition.getType()));
			}
			resultMap.put("row "+counter, resultOutput);
			counter++;
		}
		return resultMap;
	}


	//new stuff...prepared statements
	public static Object convertToActualDataType(DataType colType,Object valueObj){
		String valueObjString = valueObj+"";
		switch(colType.getName()){
		case UUID: 
			return UUID.fromString(valueObjString);
		case VARINT: 
			return BigInteger.valueOf(Long.parseLong(valueObjString));
		case BIGINT: 
			return Long.parseLong(valueObjString);
		case INT: 
			return Integer.parseInt(valueObjString);
		case FLOAT: 
			return Float.parseFloat(valueObjString);	
		case DOUBLE: 
			return Double.parseDouble(valueObjString);
		case BOOLEAN: 
			return Boolean.parseBoolean(valueObjString);
		case MAP: 
			return (Map<String,Object>)valueObj;
		default:
			return valueObjString;
		}
	}

	public void preparedInsert(String keyspace, String table, String fields, String valueHolder, 
			ArrayList<Object> values,String ttl, String timestamp,String consistency) throws Exception{
		logger.debug("In prepared insert: fields string:"+fields+"values:"+valueHolder);
		String insertQuery = "INSERT INTO "+keyspace+"."+table+" "+ fields+" values "+ valueHolder;
		
		if((ttl != null) && (timestamp != null)){
			logger.debug("both there");
			insertQuery = insertQuery + " USING TTL ? AND TIMESTAMP ?";
			values.add(Integer.parseInt(ttl));
			values.add(Long.parseLong(timestamp));
		}
		
		if((ttl != null) && (timestamp == null)){
			logger.debug("ONLY TTL there");
			insertQuery = insertQuery + " USING TTL ?";
			values.add(Integer.parseInt(ttl));
		}

		if((ttl == null) && (timestamp != null)){
			logger.debug("ONLY timestamp there");
			insertQuery = insertQuery + " USING TIMESTAMP ?";
			values.add(Long.parseLong(timestamp));
		}

		logger.info("In preprared insert: the actual insert query:"+insertQuery+"; the values"+values);
		PreparedStatement preparedInsert = session.prepare(insertQuery);
		if(consistency.equalsIgnoreCase("critical")){
			logger.info("Executing critical put query");
			preparedInsert.setConsistencyLevel(ConsistencyLevel.QUORUM);
		}
		else if (consistency.equalsIgnoreCase("eventual")){
			logger.info("Executing simple put query");
			preparedInsert.setConsistencyLevel(ConsistencyLevel.ONE);
		}
		session.execute(preparedInsert.bind(values.toArray()));
		
	}

}
