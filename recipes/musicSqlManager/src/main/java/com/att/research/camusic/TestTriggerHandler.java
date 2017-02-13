package com.att.research.camusic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.h2.api.DatabaseEventListener;
/**
* SQL Operation handler is responsible for all triggering on the sql table.
* 
*
* @author  Bharath Balasubramanian
* @version 1.0
* @since   2014-03-31 
*/

public class TestTriggerHandler implements DatabaseEventListener{
	
	

	@Override
	public void closingDatabase() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void exceptionThrown(SQLException arg0, String arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void opened() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setProgress(int arg0, String arg1, int arg2, int arg3) {
		StringTokenizer st = new StringTokenizer(arg1);
		String first = st.nextToken();
		String second = st.nextToken();
		String third = st.nextToken();
		if(first.equalsIgnoreCase("create") && second.equalsIgnoreCase("table")){
			System.out.println("table creation event!");
			System.out.println(third);
	        Connection connection = H2Example.getDBConnection();
	        try {
				Statement stmt = connection.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS C where TABLE_NAME= 'PERSON'");
				while(rs.next()){
					String columnName = rs.getString("COLUMN_NAME");
					System.out.println(columnName);
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
