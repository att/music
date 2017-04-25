package com.att.research.mdbc.unittest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.att.research.camusic.MusicSqlManager;

public class BasicTest {
	final static Logger logger = Logger.getLogger(BasicTest.class);

    public static void main(String[] args) throws Exception {
        try {
            executeQueries();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void executeQueries() throws SQLException {
        MusicSqlManager handle = new MusicSqlManager();
        try {
        	
    		
        	handle.executeSQLWrite("CREATE TABLE TEST_NO_KEYS (id int, name varchar(255))");
        	
        	//assert that no triggers were created since this table has no primary key
        	ResultSet dbRs = handle.executeSQLRead("SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TABLE_NAME='TEST_NO_KEYS'");
        	assert(dbRs.next() == false);


        	handle.executeSQLWrite("CREATE TABLE PERSON (ID_ varchar(255), name varchar(255), primary key (ID_))");
        	
        	//assert that triggers were created
        	dbRs = handle.executeSQLRead("SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TABLE_NAME='PERSON'");
        	
    		Set<String> expTriggers= new HashSet<String>();
    		expTriggers.add("TRI_INS_PERSON");  expTriggers.add("TRI_UPDATE_PERSON");   		
    		expTriggers.add("TRI_DEL_PERSON");  expTriggers.add("TRI_SEL_PERSON");

    		Set<String> actualTriggers = new HashSet<String>();
    		
     		while (dbRs.next()) 
    			actualTriggers.add(dbRs.getString("TRIGGER_NAME"));
     		
     		assert(expTriggers == actualTriggers);
    			
    		handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('1', 'Anju')");
        	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('2', 'Sonia')");
        	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('3', 'Sameer')");
        	
        	//assert that the insert populated the dirty and actual tables in music
        	com.datastax.driver.core.ResultSet musicRs = handle.executeMusicRead("select * from camunda.dirty_PERSON_0;");
        	
        	handle.executeSQLWrite("UPDATE PERSON SET NAME='Sonia Sharma' where ID_='2'");
        	
        	handle.executeSQLWrite("DELETE FROM PERSON WHERE ID_='1'");


     		dbRs = handle.executeSQLRead("select * from PERSON");

    		while (dbRs.next()) {
    			logger.info("ID_ " + dbRs.getInt("ID_") + " Name " + dbRs.getString("name"));
    		}

    		handle.executeSQLWrite("DROP table PERSON");
    		handle.executeSQLWrite("DROP table TEST_MANY_KEYS");
    		handle.executeSQLWrite("DROP table TEST_NO_KEYS");
    		
    		handle.close();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
        	handle.close();
        }
    }
}
