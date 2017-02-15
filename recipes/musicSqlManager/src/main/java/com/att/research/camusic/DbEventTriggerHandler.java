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
	 * This method gets triggered on every db event which we wish to use to capture table creation and deletion
	 * todo: capture keyspace or schema events
	 */
	public void setProgress(int arg0, String arg1, int arg2, int arg3) {
		StringTokenizer st = new StringTokenizer(arg1);
		String first = st.nextToken();
		String second = st.nextToken();
		String third = st.nextToken();
		if(first.equalsIgnoreCase("create") && second.equalsIgnoreCase("table")){
			String tableName = third.substring(0, third.indexOf("("));
			createTableEvent(tableName);
		}else if(first.equalsIgnoreCase("drop") && second.equalsIgnoreCase("table")){
			String tableName = third.substring(0, third.indexOf("("));
			new MusicSqlManager().clearMusicForTable(tableName);
		}
	}
	
	/**This method is called when a table is created in the database to enable the corresponding intializations in the
	 * music sql manager.
	 * @param tableName
	 */
	private void createTableEvent(String tableName){
		logger.info("Table creation event:table name:"+tableName);
        Connection connection = H2Example.getDBConnection();
        try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEXES C where TABLE_NAME= 'PERSON'");
			ArrayList<String> primaryKeys = new ArrayList<String>();
			while(rs.next()){
				String keyName = rs.getString("COLUMN_NAME");
				primaryKeys.add(keyName);
			}
			if((primaryKeys.size() == 1) && (primaryKeys.get(0).equals("ID_"))){
				new MusicSqlManager().initializeDbAndMusicForTable(tableName);
			}
			else
				logger.warn("Table "+tableName+" does not have supported primary keys:"+primaryKeys);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
