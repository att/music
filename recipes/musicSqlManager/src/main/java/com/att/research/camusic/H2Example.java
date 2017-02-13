package com.att.research.camusic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

// H2 In-Memory Database Example shows about storing the database contents into memory. 

public class H2Example {

    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:~/data/test;AUTO_SERVER=TRUE;DATABASE_EVENT_LISTENER="+TestTriggerHandler.class.getName();
    private static final String DB_USER = "";
    private static final String DB_PASSWORD = "";
    private static Connection dbConnection = null;
    public static void main(String[] args) throws Exception {
        try {
            insertWithStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void insertWithStatement() throws SQLException {
        Connection connection = getDBConnection();
        Statement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            stmt.execute("CREATE TABLE PERSON(id int primary key, name varchar(255))");
            stmt.execute("INSERT INTO PERSON(id, name) VALUES(1, 'Anju')");
            stmt.execute("INSERT INTO PERSON(id, name) VALUES(2, 'Sonia')");
            stmt.execute("INSERT INTO PERSON(id, name) VALUES(3, 'Asha')");

            ResultSet rs = stmt.executeQuery("select * from PERSON");
            System.out.println("H2 In-Memory Database inserted through Statement");
            while (rs.next()) {
                System.out.println("Id " + rs.getInt("id") + " Name " + rs.getString("name"));
            }

            stmt.execute("DROP TABLE PERSON");
            stmt.close();
            connection.commit();
        } catch (SQLException e) {
            System.out.println("Exception Message " + e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            connection.close();
        }
    }

    public static Connection getDBConnection() {
    	if(dbConnection == null){
    		System.out.println("Db connection null");
	        try {
	            Class.forName(DB_DRIVER);
	        } catch (ClassNotFoundException e) {
	            System.out.println(e.getMessage());
	        }
	        try {
	            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
	            return dbConnection;
	        } catch (SQLException e) {
	            System.out.println(e.getMessage());
	        }
	    	}
        return dbConnection;
    }
}
