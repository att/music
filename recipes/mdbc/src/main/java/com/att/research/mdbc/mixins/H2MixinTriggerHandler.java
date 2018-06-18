package com.att.research.mdbc.mixins;

import java.sql.Connection;
import java.sql.SQLException;

import org.h2.api.Trigger;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.MusicSqlManager;

/**
 * This implementation of the H2 Trigger interface handles triggers for in memory databases.
 * This handler is H2 specific.
 *
 * @author  Bharath Balasubramanian
 * @version 1.0
 * @since   2014-03-31
 */
public class H2MixinTriggerHandler implements Trigger {
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(H2MixinTriggerHandler.class);

	private String tableName;
	private String triggerName;
	private MusicSqlManager musicHandle;

	/**
	 * This method is called by H2 once when initializing the trigger. It is called when the trigger is created,
	 * as well as when the database is opened. The type of operation is a bit field with the appropriate flags set.
	 * As an example, if the trigger is of type INSERT and UPDATE, then the parameter type is set to (INSERT | UPDATE).
	 *
	 * @param conn a connection to the database (a system connection)
	 * @param schemaName the name of the schema
	 * @param triggerName the name of the trigger used in the CREATE TRIGGER statement
	 * @param tableName the name of the table
	 * @param before whether the fire method is called before or after the operation is performed
	 * @param type the operation type: INSERT, UPDATE, DELETE, SELECT, or a combination (this parameter is a bit field)
	 */
	@Override
	public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type)
		throws SQLException {
		this.tableName = tableName;
		this.triggerName = triggerName;
		this.musicHandle = MusicSqlManager.getMusicSqlManager(triggerName);
		if (musicHandle == null) {
			String info = String.format(" (%s %s %b %d)", schemaName, tableName, before, type);
			logger.info(EELFLoggerDelegate.applicationLogger,"No MusicSqlManager found for triggerName "+triggerName + info);
			
		} else {
			logger.info(EELFLoggerDelegate.applicationLogger,"Init Name:"+ triggerName+", table:"+tableName+", type:"+type);
		}
	}

	/**
	 * This method is called for each triggered action. The method is called immediately when the operation occurred
	 * (before it is committed). A transaction rollback will also rollback the operations that were done within the trigger,
	 * if the operations occurred within the same database. If the trigger changes state outside the database, a rollback
	 * trigger should be used.
	 * <p>The row arrays contain all columns of the table, in the same order as defined in the table.</p>
	 * <p>The trigger itself may change the data in the newRow array.</p>
	 * @param conn a connection to the database
	 * @param oldRow the old row, or null if no old row is available (for INSERT)
	 * @param newRow the new row, or null if no new row is available (for DELETE)
	 * @throws SQLException if the operation must be undone
	 */
	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
		try {
			if (musicHandle == null) {
				logger.error(EELFLoggerDelegate.errorLogger,"Trigger called but there is no MusicSqlManager found for triggerName "+triggerName);
				
			} else {
				if (oldRow == null) {
					if (newRow == null) {
						// this is a SELECT Query; just merge the table
						logger.info(EELFLoggerDelegate.applicationLogger,"In trigger fire, Trigger Name:"+ triggerName+", Operation type: SELECT");
						
						musicHandle.readDirtyRowsAndUpdateDb(tableName);
					} else {
						// this is an INSERT
						logger.info(EELFLoggerDelegate.applicationLogger,"In trigger fire, Trigger Name:"+ triggerName+", Operation type: INSERT, newrow="+cat(newRow));
						musicHandle.updateDirtyRowAndEntityTableInMusic(tableName, newRow);
					}
				} else {
					if (newRow == null) {
						// this is a DELETE
						logger.info(EELFLoggerDelegate.applicationLogger,"In trigger fire, Trigger Name:"+ triggerName+", Operation type: DELETE, oldrow="+cat(oldRow));
						musicHandle.deleteFromEntityTableInMusic(tableName, oldRow);
					} else {
						// this is an UPDATE
						logger.info(EELFLoggerDelegate.applicationLogger,"In trigger fire, Trigger Name:"+ triggerName+", Operation type: UPDATE, newrow="+cat(newRow));
						musicHandle.updateDirtyRowAndEntityTableInMusic(tableName, newRow);
					}
				}
			}
		} catch (Exception e) {
			// Ignore all exceptions in this method; not ideal but Cassandra is a p.i.t.a. with exceptions.
			logger.error(EELFLoggerDelegate.errorLogger,"Exception "+e);
			e.printStackTrace();
		}
	}
	private String cat(Object[] o) {
		StringBuilder sb = new StringBuilder("[");
		String pfx = "";
		for (Object t : o) {
			sb.append(pfx).append((t == null) ? "null" : t.toString());
			pfx = ",";
		}
		sb.append("]");
		return sb.toString();
	}
	@Override
	public void close() throws SQLException {
		// nothing
	}
	@Override
	public void remove() throws SQLException {
		// nothing
	}
}
