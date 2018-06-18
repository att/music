package com.att.research.mdbc.mixins;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.h2.api.Trigger;
import org.json.JSONObject;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.MusicSqlManager;
import com.att.research.mdbc.TableInfo;

/**
 * This Trigger handler handles triggers when an MDBC tracked table is modified on an H2 server.
 * This is the only code that MUST be included on a H2 server when used with MDBC, because H2 needs
 * to call this class for any triggers, rather than call back to MDBC (which is not possible)
 * it writes the trigger event to a table, to be read by MDBC later.
 *
 * For each table event, the trigger records the following items into the table named MDBC_TRANSLOG:
 * <ol>
 * <li>the table name</li>
 * <li>the connection ID (taken from the name of the trigger)</li>
 * <li>the operation performed; 0=delete, 1=insert, 2=update</li>
 * <li>the key column or columns, stored as JSON</li>
 * </ol>
 *
 * To invoke this trigger handler on the server, use the following SQL:
 * <pre>
 * 	CREATE TRIGGER IF NOT EXISTS T_connid_NAME AFTER DELETE|INSERT|UPDATE ON tblname
 * 	FOR EACH ROW CALL "com.att.research.mdbc.mixins.H2ServerMixinTriggerHandler"
 * </pre>
 * Be sure that mdbc.jar (containing
 * com/att/research/mdbc/mixins/H2ServerMixinTriggerHandler.class
 * com/att/research/mdbc/TableInfo.class
 * ), json.jar and log4j.jar are in the server's CLASSPATH.
 *
 * @author Robert P. Eby
 */
public class H2ServerMixinTriggerHandler implements Trigger {
	public static final String TRANS_TBL = "MDBC_TRANSLOG";
	public static final int    OP_DELETE = 0;	// TODO change to D, I, U
	public static final int    OP_INSERT = 1;
	public static final int    OP_UPDATE = 2;

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(H2ServerMixinTriggerHandler.class);
	private static final String CREATE_TBL_SQL =
		"CREATE TABLE IF NOT EXISTS "+TRANS_TBL+
		" (IX INT AUTO_INCREMENT, CONNID INT, OP INT, TABLENAME VARCHAR, KEYDATA VARCHAR, PRIMARY KEY (IX))";
	private static final String INSERT_TBL_SQL =
		"INSERT INTO "+TRANS_TBL+" (TABLENAME, CONNID, OP, KEYDATA) VALUES (?, ?, ?, ?)";
	private static Boolean table_created = false;
	private static Map<String, TableInfo> ticache = new HashMap<String, TableInfo>();

	private String tableName;
	private String triggerName;
	private int connid;
	private PreparedStatement ps = null;
	private TableInfo info;

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
		String[] s = triggerName.split("_");
		this.connid = Integer.parseInt(s[1]);
		this.info = getTableInfo(conn, tableName);
		createTransTable(conn);
		logger.debug("Init Name:"+ triggerName+", table:"+tableName+", type:"+type);
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
			System.out.println("Trigger fired");
			if (oldRow == null) {
				if (newRow == null) {
					// this is a SELECT Query; ignore
					logger.info(EELFLoggerDelegate.applicationLogger,"H2ServerMixinTriggerHandler.fire(); unexepected SELECT, triggerName+="+ triggerName);
						
				} else {
					// this is an INSERT
					logger.info(EELFLoggerDelegate.applicationLogger,"H2ServerMixinTriggerHandler.fire(); triggerName="+ triggerName+", op=INSERT, newrow="+cat(newRow));
					writeRecord(conn, OP_INSERT, newRow);
				}
			} else {
				if (newRow == null) {
					// this is a DELETE
						logger.info(EELFLoggerDelegate.applicationLogger,"H2ServerMixinTriggerHandler.fire(); triggerName="+ triggerName+", op=DELETE, oldrow="+cat(oldRow));
					writeRecord(conn, OP_DELETE, oldRow);
				} else {
					// this is an UPDATE
					
						logger.info(EELFLoggerDelegate.applicationLogger,"H2ServerMixinTriggerHandler.fire(); triggerName="+ triggerName+", op=UPDATE, newrow="+cat(newRow));
					writeRecord(conn, OP_UPDATE, newRow);
				}
			}
		} catch (Exception e) {
			// Log and ignore all exceptions
			logger.error(EELFLoggerDelegate.errorLogger,"IGNORING EXCEPTION: "+e);
			e.printStackTrace();
		}
	}
	public synchronized static void createTransTable(Connection conn) {
		try {
			Statement stmt = conn.createStatement();
			stmt.execute(CREATE_TBL_SQL);
			stmt.close();
			table_created = true;
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"createTransTable: problem creating the "+TRANS_TBL+" table! "+e);
			
		}
		table_created = true;
	}
	private void writeRecord(Connection conn, int op, Object[] row) {
		if (table_created) {
			try {
				if (ps == null) {
					ps = conn.prepareStatement(INSERT_TBL_SQL);
				}
				String keys = rowKeysToJSON(row);
				ps.setString(1, tableName);
				ps.setInt(2, connid);
				ps.setInt(3, op);
				ps.setString(4, keys);
				ps.execute();
				logger.info(EELFLoggerDelegate.applicationLogger,"writeRecord "+tableName+"+, "+connid+", "+op+", "+keys);
			} catch (SQLException e) {
				logger.error(EELFLoggerDelegate.errorLogger,"createTable: problem inserting into "+TRANS_TBL+" table!");
				
			}
		} else {
			logger.error(EELFLoggerDelegate.errorLogger,"Cannot write to the transaction table since it was not created.");
		}
	}
	// Build JSON string representing this row from the table
	private String rowKeysToJSON(Object[] row) {
		JSONObject jo = new JSONObject();
		for (int i = 0; i < row.length; i++) {
			// TODO - optimization: pass empty strings for non-key columns??
			jo.put(info.columns.get(i), row[i]);
		}
		return jo.toString();
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
	/**
	 * Return a TableInfo object for the specified table.  This method first looks in a cache of previously constructed
	 * TableInfo objects for the table. If not found, it queries the INFORMATION_SCHEMA.COLUMNS table to obtain the column
	 * names and types of the table, and the INFORMATION_SCHEMA.INDEXES table to find the primary key columns.  It creates
	 * a new TableInfo object with the results.
	 * <p>
	 * This function is very similar (although not identical) to {@link com.att.research.mdbc.mixins.H2Mixin#getTableInfo(String)},
	 * and should be kept in sync with that.
	 * </p>
	 * @param conn the Connection to the database containing the table schema
	 * @param tableName the table to look up
	 * @return a TableInfo object containing the info we need, or null if the table does not exist
	 */
	private TableInfo getTableInfo(Connection conn, String tableName) {
		TableInfo ti = ticache.get(tableName);
		if (ti == null) {
			ti = new TableInfo();
			try {
				String tbl = tableName.toUpperCase();
				String sql = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+tbl+"'";
				ResultSet rs = executeSQLRead(conn, sql);
				if (rs != null) {
					while (rs.next()) {
						String name = rs.getString("COLUMN_NAME");
						int type = rs.getInt("DATA_TYPE");
						ti.columns.add(name);
						ti.coltype.add(type);
						ti.iskey.add(false);
					}
					rs.getStatement().close();
					sql = "SELECT COLUMN_NAME, PRIMARY_KEY FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='"+tbl+"'";
					rs = executeSQLRead(conn, sql);
					if (rs != null) {
						while (rs.next()) {
							boolean b = rs.getBoolean("PRIMARY_KEY");
							if (b) {
								String name = rs.getString("COLUMN_NAME");
								for (int i = 0; i < ti.columns.size(); i++) {
									if (ti.columns.get(i).equals(name)) {
										ti.iskey.set(i, true);
									}
								}
							}
						}
						rs.getStatement().close();
					}
				} else {
					logger.info(EELFLoggerDelegate.applicationLogger,"Cannot retrieve table info for table "+tableName+" from H2.");
				}
			} catch (SQLException e) {
				logger.error(EELFLoggerDelegate.errorLogger,"Cannot retrieve table info for table "+tableName+" from H2: "+e);
				return null;
			}
			ticache.put(tableName, ti);
		}
		return ti;
	}
	private ResultSet executeSQLRead(Connection conn, String sql) {
		logger.info(EELFLoggerDelegate.applicationLogger,"Executing SQL read:"+ sql);
		
		ResultSet rs = null;
		try {
			Statement stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"Executing SQL read:"+ sql);
		}
		return rs;
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
