package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.music.datastore.CassaDataStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;

import com.datastax.driver.core.ResultSet;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMusicCore {
	
    static CassaDataStore dataStore;
    String keyspace = "MusicCoreUnitTestKp";
    String table = "SampleTable";

    @BeforeClass
    public static void init() {
        dataStore = CassandraCQL.connectToEmbeddedCassandra();       
        MusicCore.mDstoreHandle = dataStore;


    }
    
    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
        dataStore.close();
    }

    @Test
    public void Test1_createKeyspace() throws MusicServiceException, MusicQueryException {

        Map<String,Object> replicationInfo = new HashMap<String, Object>();
        replicationInfo.put("'class'", "'SimpleStrategy'");
        replicationInfo.put("'replication_factor'", 1);
        
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
          "CREATE KEYSPACE " + keyspace + " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));
        MusicCore.nonKeyRelatedPut(queryObject, "eventual");
        
        
        //check with the system table in cassandra
        queryObject = new PreparedQueryObject();
        String systemQuery = "SELECT keyspace_name FROM system_schema.keyspaces where keyspace_name='"+keyspace.toLowerCase()+"';";
        queryObject.appendQueryString(systemQuery);
        ResultSet rs = dataStore.executeEventualGet(queryObject);     
        assert rs.all().size()> 0;
    }
    
    @Test
    public void Test1_createTable() throws MusicServiceException, MusicQueryException {
    		PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
          "CREATE TABLE " + keyspace + "." + table + " (name text PRIMARY KEY, count varint);");
        MusicCore.createTable(keyspace, table, queryObject, "eventual");              

        
        //check with the system table in cassandra
        queryObject = new PreparedQueryObject();
        String systemQuery = "SELECT table_name FROM system_schema.tables where keyspace_name='"+keyspace.toLowerCase()+"' and table_name='"+table.toLowerCase()+"';";
        queryObject.appendQueryString(systemQuery);
        ResultSet rs = dataStore.executeEventualGet(queryObject);
        assert rs.all().size()> 0;
    }


}
