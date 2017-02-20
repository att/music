package com.att.research.mdbc.unittest;

import java.sql.SQLException;

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
        	handle.executeSQLWrite("CREATE TABLE TEST_MANY_KEYS (id int, name varchar(255), primary key (id,name))");
    		
        	handle.executeSQLWrite("CREATE TABLE TEST_NO_KEYS (id int, name varchar(255))");

        	handle.executeSQLWrite("CREATE TABLE PERSON (ID_ varchar(255), name varchar(255), primary key (ID_))");
    	
        	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('1', 'Anju')");
        	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('2', 'Sonia')");
        	handle.executeSQLWrite("INSERT INTO PERSON (ID_, name) VALUES('3', 'Asha')");

     		java.sql.ResultSet rs = handle.executeSQLRead("select * from PERSON");

    		while (rs.next()) {
    			logger.info("ID_ " + rs.getInt("ID_") + " Name " + rs.getString("name"));
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
