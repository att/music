package com.att.research.music.rest;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.apache.log4j.Logger;

import com.att.research.music.datastore.jsonobjects.JsonDelete;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.datastore.jsonobjects.JsonUpdate;
import com.att.research.music.main.MusicCore;
import com.datastax.driver.core.ResultSet;

@Path("/priorityq/")
public class RestMusicQAPI {
	
	final static Logger logger = Logger.getLogger(RestMusicQAPI.class);
	@POST
	@Path("/keyspaces/{keyspace}/{qname}")
	@Consumes(MediaType.APPLICATION_JSON)
	public void createQ(JsonTable tableObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		new RestMusicDataAPI().createTable(tableObj, keyspace, tablename);
	}

	@POST
	@Path("/keyspaces/{keyspace}/{qname}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public void insertIntoQ(JsonInsert insObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{
		new RestMusicDataAPI().insertIntoTable(insObj, keyspace, tablename);
	}

	@PUT
	@Path("/keyspaces/{keyspace}/{qname}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public boolean updateQ(JsonUpdate updateObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{
		return new RestMusicDataAPI().updateTable(updateObj, keyspace, tablename, info);
	}

	@DELETE
	@Path("/keyspaces/{keyspace}/{qname}/rows")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public boolean deleteFromQ(JsonDelete delObj, @PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info) throws Exception{ 
		return new RestMusicDataAPI().deleteFromTable(delObj, keyspace, tablename, info);
	}

	@GET
	@Path("/keyspaces/{keyspace}/{qname}/peek")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> peek(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		int limit =1; //peek must return just the top row
		String query = new RestMusicDataAPI().selectSpecificQuery(keyspace,tablename,info,limit);
		ResultSet results = MusicCore.get(query);
		return MusicCore.marshallResults(results);

	} 

	@GET
	@Path("/keyspaces/{keyspace}/{qname}/filter")
	@Produces(MediaType.APPLICATION_JSON)	
	public Map<String, HashMap<String, Object>> filter(@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename, @Context UriInfo info){
		int limit =-1; 
		String query = new RestMusicDataAPI().selectSpecificQuery(keyspace,tablename,info,limit);
		ResultSet results = MusicCore.get(query);
		return MusicCore.marshallResults(results);
	} 

	@DELETE
	@Path("/keyspaces/{keyspace}/{qname}")
	public void dropQ(JsonTable tabObj,@PathParam("keyspace") String keyspace, @PathParam("tablename") String tablename) throws Exception{ 
		new RestMusicDataAPI().dropTable(tabObj, keyspace, tablename);
	}
}
