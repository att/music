package com.att.research.camusic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

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
	
	
	public static void main(String[] args){
		//testing the triggers	
		try {
			Connection con = DriverManager.getConnection("jdbc:h2:mem:camunda", "sa", "" );
			java.sql.Statement stmt = con.createStatement();
			stmt.execute("CREATE TABLE TRIGGER_TEST_TABLE (COL1 VARCHAR, COL2 INTEGER)");
			stmt.execute("CREATE TRIGGER IF NOT EXISTS TRI_INS AFTER INSERT ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+TestTriggerHandler.class.getName()+"\"");
			stmt.execute("CREATE TRIGGER IF NOT EXISTS TRI_INS AFTER INSERT ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+TestTriggerHandler.class.getName()+"\"");
			stmt.execute("CREATE TRIGGER TRI_UP AFTER UPDATE ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+TestTriggerHandler.class.getName()+"\"");
			stmt.execute("CREATE TRIGGER TRI_DEL AFTER DELETE ON TRIGGER_TEST_TABLE FOR EACH ROW CALL \""+TestTriggerHandler.class.getName()+"\"");

			
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
		System.out.println("Event!!!"+arg0+"|"+arg1+"|"+arg2+"|"+arg3);
	}

}
