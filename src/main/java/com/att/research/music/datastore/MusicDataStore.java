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

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
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
	public MusicDataStore(){
		System.out.println("Connecting to cass cluster..");
		connectToCassaCluster();
	}

	public MusicDataStore(String remoteIp){
		System.out.println("Connecting to cass cluster at "+remoteIp);
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
		System.out.println("wrong");
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		String address= "localhost";
		System.out.println("Iterating through possible ips.."+getAllPossibleLocalIps());
		while(it.hasNext()){
			try {
				cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
				//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
				Metadata metadata = cluster.getMetadata();
				System.out.printf("Connected to cluster: %s\n", 
						metadata.getClusterName());
				for ( Host host : metadata.getAllHosts() ) {
					System.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
							host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
				}
				session = cluster.connect();
				break;
			} catch (NoHostAvailableException e) {
				System.out.println("Cant find host:"+ address);
				address= it.next();
			} 
		}
	}
	private void connectToCassaCluster(String address){	
		cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
		//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n", 
				metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
					host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
		}
		session = cluster.connect();
	}

	public ResultSet executeGetQuery(String query){
		System.out.println("Executing normal get query");
		long start = System.currentTimeMillis();
		Statement statement = new SimpleStatement(query);
		statement.setConsistencyLevel(ConsistencyLevel.ONE);
		ResultSet results = session.execute(statement);
		long end = System.currentTimeMillis();
		System.out.println("time taken for actual get in cassandra:"+(end-start));
		return results;	
	}

	public ResultSet executeCriticalGetQuery(String query){
		Statement statement = new SimpleStatement(query);
		System.out.println("Executing critical get query");
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

	public void executePutQuery(String query, String consistency){
		System.out.println("in data store handle, executing put..");
		long start = System.currentTimeMillis();
		Statement statement = new SimpleStatement(query);
		if(consistency.equalsIgnoreCase("atomic")){
			System.out.println("Executing critical put query");
			statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		}
		else if (consistency.equalsIgnoreCase("eventual")){
			System.out.println("Executing simple put query");
			statement.setConsistencyLevel(ConsistencyLevel.ONE);
		}
		session.execute(statement); 
		long end = System.currentTimeMillis();
		System.out.println("time taken for actual put in cassandra:"+(end-start));
	}

	public void createSchema() { 
		Statement statement = new SimpleStatement("CREATE KEYSPACE IF NOT EXISTS bharath WITH replication " + 
				"= {'class':'SimpleStrategy', 'replication_factor':3};");
		statement.setConsistencyLevel(ConsistencyLevel.ALL);
		session.execute(statement);

		//session.execute("CREATE KEYSPACE IF NOT EXISTS bharath WITH replication " + 
		//      "= {'class':'SimpleStrategy', 'replication_factor':1};");

		session.execute(
				"CREATE TABLE IF NOT EXISTS bharath.songs (" +
						"id uuid PRIMARY KEY," + 
						"title text," + 
						"album text," + 
						"artist text," + 
						"tags set<text>," + 
						"data blob" + 
				");");
		session.execute(
				"CREATE TABLE IF NOT EXISTS bharath.playlists (" +
						"id uuid," +
						"title text," +
						"album text, " + 
						"artist text," +
						"song_id uuid," +
						"PRIMARY KEY (id, title, album, artist)" +
				");");

	}

	public void loadData() { 
		session.execute(
				"INSERT INTO bharath.songs (id, title, album, artist, tags) " +
						"VALUES (" +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye out Blackbird'," +
						"'Joséphine Baaaker'," +
						"{'jazz', '2013'})" +
				";");
		session.execute(
				"INSERT INTO bharath.playlists (id, song_id, title, album, artist) " +
						"VALUES (" +
						"2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye out Blackbird'," +
						"'Joséphine Baaaker'" +
				");");
	}

	public void querySchema(){
		ResultSet results = session.execute("SELECT * FROM bharath.playlists " +
				"WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
				"-------------------------------+-----------------------+--------------------"));
		for (Row row : results) {
			System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
					row.getString("album"),  row.getString("artist")));
		}
		System.out.println();
	}

	public Object readRow(Row row, String name, DataType colType){	
		switch(colType.getName()){
		case VARCHAR: 
			return row.getString(name);
		case UUID: 
			return row.getUUID(name);
		case VARINT: 
			return row.getVarint(name);
		case BIGINT: 
			return row.getLong(name);
		case INT: 
			return row.getInt(name);
		case FLOAT: 
			return row.getFloat(name);	
		case DOUBLE: 
			return row.getDouble(name);
		case BOOLEAN: 
			return row.getBool(name);
		case MAP: 
			return row.getMap(name, String.class, String.class);
		default: 
			return null;
		}
	}

	public Map<String, HashMap<String, Object>> marshalData(ResultSet results){
		Map<String, HashMap<String, Object>> resultMap = new HashMap<String, HashMap<String,Object>>();
		int counter =0;
		for (Row row : results) {
			ColumnDefinitions colInfo = row.getColumnDefinitions();
			HashMap<String,Object> resultOutput = new HashMap<String, Object>();
			for (Definition definition : colInfo) {
				//	System.out.println("column name:"+ definition.getName());
				if(!definition.getName().equals("vector_ts"))
					resultOutput.put(definition.getName(), readRow(row, definition.getName(), definition.getType()));
			}
			resultMap.put("row "+counter, resultOutput);
			counter++;
		}
		return resultMap;
	}

//	public void close() {
//		session.close();
//		cluster.close();
//	}
	public static void main(String[] args) {
		String remoteIp = "135.197.226.98";
		MusicDataStore client =null;
		client = new MusicDataStore(remoteIp);
		client.createSchema();
		client.loadData();
		client.querySchema();
	}
}
