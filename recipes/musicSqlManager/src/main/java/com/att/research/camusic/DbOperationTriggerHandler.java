package com.att.research.camusic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;
/**
* SQL Operation handler is responsible for all triggering on the sql table.
* 
*
* @author  Bharath Balasubramanian
* @version 1.0
* @since   2014-03-31 
*/

public class DbOperationTriggerHandler implements org.h2.api.Trigger{
	MusicSqlManager musicHandle;
	String tableName;
	String triggerName;
	final static Logger logger = Logger.getLogger(DbOperationTriggerHandler.class);

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
		if((oldRow ==null) && (newRow == null)){//this is a select Query; just merge the table
			logger.info("-----------------------------------------------------");
			logger.info("In trigger fire, Trigger Name:"+ triggerName+", Operation type: select");
			musicHandle.readDirtyRowsAndUpdateDb(tableName);
		}else
		if(oldRow == null){//this is an insert
			logger.info("-----------------------------------------------------");
			String rowKey = (String)newRow[0];//key of inserted row
			logger.info("In trigger fire, Trigger Name:"+ triggerName+", Operation type: insert");
			musicHandle.updateDirtyRowAndEntityTableInMusic(tableName, newRow,rowKey);
			return; 
		}else 
		if(newRow == null){	//this is a delete
			logger.info("-----------------------------------------------------");
			String rowKey = (String)oldRow[0];//key of deleted row
			logger.info("In trigger fire, Trigger Name:"+ triggerName+", Operation type: delete");
			musicHandle.deleteFromEntityTableInMusic(tableName,rowKey);		
			return;
		}else{//this is an update
			if(MusicSqlManager.getIsUpdateInProgress())//to avoid cycylical updates
				MusicSqlManager.setIsUpdateInProgress(false);
			else{
				logger.info("-----------------------------------------------------");
				String rowKey = (String)oldRow[0];//key of updated row
				logger.info("In trigger fire, Trigger Name:"+ triggerName+", Operation type: update");
				musicHandle.updateDirtyRowAndEntityTableInMusic(tableName, newRow,rowKey);}
		}	}

	@Override
	public void close() throws SQLException {}
	public void remove() throws SQLException {}
}
