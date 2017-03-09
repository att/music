package com.att.research.camusic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;
import org.h2.api.DatabaseEventListener;

/**
* SQL Operation handler is responsible for all triggering on the sql table.
* 
*
* @author  Bharath Balasubramanian
* @version 1.0
* @since   2014-03-31 
*/

public class DbEventTriggerHandler implements DatabaseEventListener{
	
	final static Logger logger = Logger.getLogger(DbEventTriggerHandler.class);


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
	/**
	 * This method gets triggered on every db event which we wish to use to capture table creation. Currently does not support capture keyspace/schema/table drop events.
	 * Drops are hard, because we do not want to drop a table until it is committed in a transaction and that is hard to capture. 
	 * 
	 */
	public void setProgress(int arg0, String arg1, int arg2, int arg3) {
		StringTokenizer st = new StringTokenizer(arg1);
		String first = st.nextToken();
		String second = st.nextToken();
		String third = st.nextToken();
		if(first.equalsIgnoreCase("create") && second.equalsIgnoreCase("table")){
			String tableName = third;
			logger.info("the db event:"+arg0+"|"+arg1+"|"+arg2+"|"+arg3);
			try {
				logger.info("In set progress, the table name:"+tableName);
				ResultSet rs = new MusicSqlManager().executeSQLRead("SELECT * FROM INFORMATION_SCHEMA.TABLES where TABLE_NAME= '"+tableName+"'");
				if(rs.next())
					createTableEvent(tableName);
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	/* 	
	 * else if(first.equalsIgnoreCase("drop") && second.equalsIgnoreCase("table")){
			String tableName = third.substring(0, third.indexOf("("));
			new MusicSqlManager().clearMusicForTable(tableName);
		}*/ 
	}
	
	/**This method is called when a table is created in the database to enable the corresponding intializations in the
	 * music sql manager.
	 * @param tableName
	 */
	private void createTableEvent(String tableName){
		logger.info("Real table creation event:"+tableName);
        try {       	
			ResultSet rs = new MusicSqlManager().executeSQLRead("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES C where TABLE_NAME= '"+tableName+"'");
			ArrayList<String> primaryKeys = new ArrayList<String>();
			while(rs.next()){
				String keyName = rs.getString("COLUMN_NAME");
				primaryKeys.add(keyName);
			}
			if((primaryKeys.size() == 1) && (primaryKeys.get(0).equals("ID_"))){
				logger.info("-----------------------------------------------------");
				logger.info("Table "+tableName+" created, primary keys:"+primaryKeys);
				new MusicSqlManager().initializeDbAndMusicForTable(tableName);
			}
			else
				logger.info("Table creation event, table "+tableName+" ignored since it does not have supported primary keys :"+primaryKeys);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
