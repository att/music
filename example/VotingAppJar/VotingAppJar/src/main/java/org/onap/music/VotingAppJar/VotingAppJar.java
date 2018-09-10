package org.onap.music.VotingAppJar;

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
public class VotingAppJar 
{
       String keyspaceName;
       String tableName;
       
       public VotingAppJar() throws MusicServiceException {
              keyspaceName = "VotingAppForMusic"+System.currentTimeMillis();
              tableName = "votecount";
              
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
                     if (e.getMessage().equals("Keyspace votingappformusic already exists")) {
                           // ignore
                     } else {
                           throw(e);
                     }                   
              }
       }
       
    private void createVotingTable() throws MusicServiceException {
       PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "CREATE TABLE " + keyspaceName + "." + tableName + " (name text PRIMARY KEY, count varint);");
              
              try {
                     MusicCore.createTable(keyspaceName, tableName, queryObject, "eventual");              
              } catch (MusicServiceException e) {
                     if (e.getMessage().equals("Table votingappformusic.votevount already exists")) {
                           //ignore
                     } else {
                           throw(e);
                     }
              }
       }
 
       private void createEntryForCandidate(String candidateName) throws MusicServiceException {
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, count) "
                            + "VALUES ('"+candidateName+"', 0);");
              
              MusicCore.nonKeyRelatedPut(queryObject, "eventual");
       }
 
       public void vote() throws MusicLockingException, MusicQueryException, MusicServiceException {
              updateVoteCount("Popeye",5);
              updateVoteCount("Judy",7);
              updateVoteCount("Mickey",8);
              updateVoteCount("Flash",2);
       }
       
       private void updateVoteCount(String candidateName, int numVotes) throws MusicLockingException, MusicQueryException, MusicServiceException {
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString(
                "INSERT INTO " + keyspaceName + "." + tableName + " (name, count) "
                            + "VALUES ('"+candidateName+"', "+numVotes+");");
              MusicCore.atomicPut(keyspaceName, tableName, candidateName, queryObject, null);
       }
 
       private void readAllVotes() throws MusicServiceException {
              PreparedQueryObject queryObject = new PreparedQueryObject();
              queryObject.appendQueryString("SELECT * FROM " + keyspaceName + "." + tableName);
              ResultSet rs = MusicCore.get(queryObject);
              for(Row candidate : rs.all()) {
                     System.out.println(candidate.getString("name") + " - " + candidate.getVarint("count"));
              }
       }
       
       public static void main( String[] args ) throws Exception {
    	   VotingAppJar vHandle = new VotingAppJar();
        vHandle.vote();
        vHandle.readAllVotes();
    }
 
}