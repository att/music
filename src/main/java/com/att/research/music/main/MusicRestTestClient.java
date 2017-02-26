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

import java.util.ArrayList;
import java.util.HashMap;

import javax.ws.rs.core.MediaType;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MusicRestTestClient {
	Client clientHandle;
	String url="http://"+MusicUtil.musicRestIp+":8080/MUSIC/rest/core/";
	public Client getClientHandle(){
		if(clientHandle == null){
			ClientConfig clientConfig = new DefaultClientConfig();
	
			clientConfig.getFeatures().put(
					JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
	
			clientHandle = Client.create(clientConfig);
		}
		
		return clientHandle;
	}
	
	public void testVersion(){
		
		WebResource webResource = getClientHandle().resource(url+"version");

		WebResource.Builder wb = webResource.accept(MediaType.TEXT_PLAIN);
		
		ClientResponse response = wb.get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

		System.out.println("Server response .... \n");
		System.out.println(output);

	}
	
	
	public void createSampleData() { 
		
		testExecutePutQuery("CREATE KEYSPACE IF NOT EXISTS bharath WITH replication " + 
			      "= {'class':'SimpleStrategy', 'replication_factor':1};");
		
		testExecutePutQuery(
			      "CREATE TABLE IF NOT EXISTS bharath.songs (" +
			            "id uuid PRIMARY KEY," + 
			            "title text," + 
			            "album text," + 
			            "artist text," + 
			            "tags set<text>," + 
			            "data blob" + 
			            ");");
			testExecutePutQuery(
			      "CREATE TABLE IF NOT EXISTS bharath.playlists (" +
			            "id uuid," +
			            "title text," +
			            "album text, " + 
			            "artist text," +
			            "song_id uuid," +
			            "PRIMARY KEY (id, title, album, artist)" +
			            ");");
			testExecutePutQuery(
				      "INSERT INTO bharath.songs (id, title, album, artist, tags) " +
				      "VALUES (" +
				          "856716f7-2e54-4715-9f00-91dcbea6cf50," +
				          "'Ma Petite Tonkinoise'," +
				          "'Mye out Blackbird'," +
				          "'Moséphine Baaaker'," +
				          "{'jazz', '2013'})" +
				          ";");
				testExecutePutQuery(
				      "INSERT INTO bharath.playlists (id, song_id, title, album, artist) " +
				      "VALUES (" +
				          "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
				          "856716f7-2e54-4715-9f00-91dcbea6cf50," +
				          "'Ma Petite Tonkinoise'," +
				          "'Mye out Blackbird'," +
				          "'Moséphine Baaaker'" +
				          ");");
	
	}
	
	public void testExecuteGetQuery(){
		String query ="SELECT * FROM bharath.playlists;";

		ArrayList<String> queryRetreivalObject = new ArrayList<String>();
		queryRetreivalObject.add(query);
		queryRetreivalObject.add("artist");
		queryRetreivalObject.add("album");

		WebResource webResource = clientHandle
				.resource(url+"executeGetQuery");

		ClientResponse response = webResource.accept("application/json").type("application/json").post(ClientResponse.class, queryRetreivalObject);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		@SuppressWarnings("unchecked")
		ArrayList<String> output = response.getEntity(ArrayList.class);

		System.out.println("Server response .... \n");
		System.out.println(output);

	}
	
	public void testExecutePutQuery(String query){
		
		WebResource webResource = clientHandle
				.resource(url+"executePutQuery");

		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, query);

		
		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}
	
	public String testCreateLock(String lockName){
		WebResource webResource = clientHandle
				.resource(url+"createLock");

		ClientResponse response = webResource.accept("application/json").type("application/json").post(ClientResponse.class, lockName);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);

		System.out.println("Server response .... \n");
		System.out.println(output);
		return output;
	}
	
	public boolean testAccquireLock(String lockId){
		WebResource webResource = clientHandle
				.resource(url+"accquireLock");

		ClientResponse response = webResource.accept("application/json").type("application/json").post(ClientResponse.class, lockId);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}

		String output = response.getEntity(String.class);
		Boolean status = Boolean.parseBoolean(output);
		System.out.println("Server response .... \n");
		System.out.println(output);
		return status;
	}
	
	public void testUnlock(String lockId){
		WebResource webResource = clientHandle
				.resource(url+"unLock");

		ClientResponse response = webResource.type("application/json").post(ClientResponse.class, lockId);

		
		if (response.getStatus() != 204) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
	}
	public static void main(String[] args){
		MusicRestTestClient mTest = new MusicRestTestClient();
		
		mTest.testVersion();

	String lockId = mTest.testCreateLock("restMusicLock1");
		System.out.println(lockId);
		
		mTest.testAccquireLock(lockId);
		
		
		String lockId1 = mTest.testCreateLock("restMusicLock1");
		System.out.println(lockId1);
		
		
		
		mTest.testAccquireLock(lockId1);
		
		mTest.testUnlock(lockId);
		mTest.testAccquireLock(lockId1);
		mTest.testUnlock(lockId1);

		
		mTest.createSampleData();
	
		mTest.testExecuteGetQuery();
	}
}
