package org.onap.music.unittests;

import java.util.HashMap;
import java.util.Map;
 
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
 
/**
 *
 */
public class TestVotingApp 
{
    String keyspaceName;
    String tableName;
       
    public TestVotingApp() throws MusicServiceException {
        keyspaceName = "VotingAppForMusic"+System.currentTimeMillis();
        tableName = "votecount";
    }

    private void initialize() throws MusicServiceException {
        createVotingKeyspace();
        System.out.println("Created keyspaces");
        createVotingTable();
        System.out.println("Created tables");

        createEntryForCandidate("Popeye");
        createEntryForCandidate("Judy");
        createEntryForCandidate("Flash");
        createEntryForCandidate("Mickey");
        System.out.println("Created candidates");
    }

    private void createVotingKeyspace() throws MusicServiceException {
              Map<String,Object> replicationInfo = new HashMap<String, Object>();
              replicationInfo.put("'class'", "'SimpleStrategy'");
              replicationInfo.put("'replication_factor'", 1);
              
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "CREATE KEYSPACE " + keyspaceName + " WITH REPLICATION = " + replicationInfo.toString().replaceAll("=", ":"));
              
              try {
                     MusicCore.nonKeyRelatedPut(queryObject, "eventual");
              } catch (MusicServiceException e) {
                   throw(e);
              }
       }
       
    private void createVotingTable() throws MusicServiceException {
        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString(
                "CREATE TABLE " + keyspaceName + "." + tableName + " (name text PRIMARY KEY, count int);");

        try {
            MusicCore.createTable(keyspaceName, tableName, queryObject, "eventual");
        } catch (MusicServiceException e) {
            throw (e);
        }
    }
 
       private void createEntryForCandidate(String candidateName) throws MusicServiceException {
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, count) "
                            + "VALUES ('"+candidateName+"', 0);");
              
              MusicCore.nonKeyRelatedPut(queryObject, "eventual");
       }
 
       
       private void updateVoteCount(String candidateName, int numVotes) throws MusicLockingException, MusicQueryException, MusicServiceException {
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "UPDATE " + keyspaceName + "." + tableName + " SET count="+numVotes + " where name='"+candidateName+"';");
              MusicCore.atomicPut(keyspaceName, tableName, candidateName, queryObject, null);
       }
 
       private HashMap<String, Integer> readAllVotes() throws MusicServiceException {
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString("SELECT * FROM " + keyspaceName + "." + tableName);
              ResultSet rs = MusicCore.get(queryObject);
              HashMap<String, Integer> voteCount = new HashMap<String, Integer>();
              for(Row candidate : rs.all()) {
            	  	voteCount.put(candidate.getString("name"), candidate.getInt("count"));
              }
              return voteCount;
       }
       
       public static void main( String[] args ) throws Exception {
        TestVotingApp tva = new TestVotingApp();
        tva.initialize();

        tva.updateVoteCount("Popeye",5);
        tva.updateVoteCount("Judy",9);
        tva.updateVoteCount("Mickey",8);
       tva.updateVoteCount("Flash",1);
       tva.updateVoteCount("Flash",2);

        HashMap<String, Integer> voteCount = tva.readAllVotes();
        System.out.println(voteCount);
		assert(voteCount.get("Popeye") == 5);
		assert(voteCount.get("Judy") == 9);
		assert(voteCount.get("Mickey") == 8);
		assert(voteCount.get("Flash") == 2);
    }
 
}