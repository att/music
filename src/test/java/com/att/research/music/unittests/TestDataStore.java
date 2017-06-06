package com.att.research.music.unittests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.MusicDataStore;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class TestDataStore {
	final static Logger logger = Logger.getLogger(TestDataStore.class);
	public static void main(String[] args) {
		String fileLocation = args[0];
		String dsList="";
	      try {
	            File f = new File(fileLocation);
	            BufferedReader b = new BufferedReader(new FileReader(f));
	            String readLine = "";
	            while ((readLine = b.readLine()) != null) {
	            	dsList = dsList + " "+ readLine;
	            	System.out.println("ip:"+readLine);
	    			MusicDataStore client = new MusicDataStore(readLine);
	    			runTest(client);
	            }
	            b.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        }	
	}
	
	public static void runTest(MusicDataStore client){
		String query = "CREATE KEYSPACE IF NOT EXISTS bharath WITH replication = "
				+ "{'class':'SimpleStrategy', 'replication_factor':1};";
		
		client.executePut(query, "eventual");
		
		query = "CREATE TABLE IF NOT EXISTS bharath.songs (" +
				"id uuid PRIMARY KEY," + 
				"title text," + 
				"album text," + 
				"artist text," + 
				"tags set<text>," + 
				"data blob" + 
		");";
		client.executePut(query, "eventual");
		
		query = "CREATE TABLE IF NOT EXISTS bharath.playlists (" +
				"id uuid," +
				"title text," +
				"album text, " + 
				"artist text," +
				"song_id uuid," +
				"PRIMARY KEY (id, title, album, artist)" +
		");";
		
		client.executePut(query, "eventual");
		
		query = "INSERT INTO bharath.songs (id, title, album, artist, tags) " +
						"VALUES (" +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye out Blackbird'," +
						"'Joséphine Baaaker'," +
						"{'jazz', '2013'})" +
				";";
		
		client.executePut(query, "eventual");

		query = "INSERT INTO bharath.playlists (id, song_id, title, album, artist) " +
						"VALUES (" +
						"2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
						"756716f7-2e54-4715-9f00-91dcbea6cf50," +
						"'La Petite Tonkinoise'," +
						"'Bye out Blackbird'," +
						"'Joséphine Baaaker'" +
				");";

		client.executePut(query, "eventual");

		ResultSet results = client.executeEventualGet("SELECT * FROM bharath.playlists " +
				"WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		
		for (Row row : results) {
			System.out.println(row.getString("title")+" "+
					row.getString("album")+" "+row.getString("artist"));
		}
	}

	}



