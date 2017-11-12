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
import com.att.research.music.main.MusicCore;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.TableMetadata;

/*
 *  API to benchmark MUSIC
 * 
 */
@Path("/benchmarks/")
public class RestMusicBmAPI {
	final static Logger logger = Logger.getLogger(RestMusicBmAPI.class);

	//music_ev_put
	//music_atomic_put
	//zk_atomic_put
	//music_ev_get
	//music_atomic_get
	//zk_atomic_get

	

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
		MusicCore.pureZkWrite(nodeName, insObj.serialize());
	}

	@GET
	@Path("/purezk/{name}")
	@Consumes(MediaType.TEXT_PLAIN)
	public byte[] pureZkGet(@PathParam("name") String nodeName) throws Exception{
		return MusicCore.pureZkRead(nodeName);
	}
}
