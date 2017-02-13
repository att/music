package com.att.research.camusic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
/**
* SQL Operation handler is responsible for all triggering on the sql table.
* 
*
* @author  Bharath Balasubramanian
* @version 1.0
* @since   2014-03-31 
*/

public class SQLTriggerHandler implements org.h2.api.Trigger{
	MusicSqlManager musicHandle;
	String tableName;
	String triggerName;

	/* (non-Javadoc)
	 * @see org.h2.api.Trigger#init(java.sql.Connection, java.lang.String, java.lang.String, java.lang.String, boolean, int)
	 */
	@Override
	public void init(Connection arg0, String schemaName, String triggerName, String tableName,
			boolean arg4, int arg5) throws SQLException {
		//basically have to treat this like a constructor
			this.tableName = tableName;
			this.triggerName = triggerName;
			musicHandle = new MusicSqlManager();
	}

	@Override
	public void fire(Connection arg0, Object[] oldRow, Object[] newRow)
			throws SQLException {
		System.out.println("-----Trigger Name:---"+ triggerName+" "+ oldRow+"|"+newRow);
		if((oldRow ==null) && (newRow == null)){//this is a select Query; just merge the table
			musicHandle.readDirtyRowsAndUpdateDb(tableName);
		}else
		if(oldRow == null){//this is an insert
			String rowKey = (String)newRow[0];//key of inserted row
			System.out.println("*********In trigger fire:"+ tableName + "-insert**********");
			musicHandle.updateDirtyRowAndEntityTableInMusic(tableName, newRow,rowKey);
			return; 
		}else 
		if(newRow == null){	//this is a delete
			String rowKey = (String)oldRow[0];//key of deleted row
			System.out.println("*********In trigger fire:"+ tableName + "-delete**********");
			musicHandle.deleteFromEntityTableInMusic(tableName,rowKey);		
			return;
		}else{//this is an update
			String rowKey = (String)oldRow[0];//key of updated row
			System.out.println("*********In trigger fire:"+ tableName + "-update**********");
			musicHandle.updateDirtyRowAndEntityTableInMusic(tableName, newRow,rowKey);
		}	}

	@Override
	public void close() throws SQLException {}
	public void remove() throws SQLException {}
	
	public static void main(String[] args){
		//testing the triggers	
		try {
			Connection con = DriverManager.getConnection("jdbc:h2:mem:camunda", "sa", "" );
			java.sql.Statement stmt = con.createStatement();
			stmt.execute("CREATE TABLE TRIGGER_TEST_TABLE (COL1 VARCHAR, COL2 INTEGER)");
			stmt.execute("CREATE TRIGGER IF NOT EXISTS TRI_INS AFTER INSERT ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+SQLTriggerHandler.class.getName()+"\"");
			stmt.execute("CREATE TRIGGER IF NOT EXISTS TRI_INS AFTER INSERT ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+SQLTriggerHandler.class.getName()+"\"");
			stmt.execute("CREATE TRIGGER TRI_UP AFTER UPDATE ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+SQLTriggerHandler.class.getName()+"\"");
			stmt.execute("CREATE TRIGGER TRI_DEL AFTER DELETE ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+SQLTriggerHandler.class.getName()+"\"");

			
			stmt.execute("INSERT INTO  TRIGGER_TEST_TABLE VALUES('bharath',25)");
			stmt.execute("INSERT INTO  TRIGGER_TEST_TABLE VALUES('mini',30)");
			stmt.execute("INSERT INTO  TRIGGER_TEST_TABLE VALUES('bini',30)");


			stmt.execute("UPDATE TRIGGER_TEST_TABLE SET COL1='thbh' where COL2=25");
			
			stmt.execute("delete from TRIGGER_TEST_TABLE where COL2=30");
			
			String query = "select * from TRIGGER_TEST_TABLE";
			java.sql.ResultSet rs = stmt.executeQuery(query);

			while (rs.next()) {
				for(int columnIndex =1; columnIndex < 3;++columnIndex){
					Object sid = rs.getObject(columnIndex);
					if(sid != null)
						System.out.print(sid.getClass()+" "+sid+"|");
				}
				System.out.println();
			    // Do whatever you want to do with these 2 values
			}			

		
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
