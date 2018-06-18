package com.att.research.mdbc.mixins;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.MusicSqlManager;
import com.att.research.mdbc.TableInfo;

/**
 * This class provides the methods that MDBC needs in order to mirror data to/from an H2 Database
 * instance running locally; either in a local file or an in memory DB.
 *
 * @author Robert P. Eby
 */
public class H2Mixin implements DBInterface {
	public  static final String MIXIN_NAME = "h2";
	private static final String triggerClassName = H2MixinTriggerHandler.class.getName();
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(H2Mixin.class);
	private static int connindex = 0;

	protected final MusicSqlManager msm;
	//protected final Logger logger;
	protected final int connId;
	protected final Connection dbConnection;
	protected final Map<String, TableInfo> tables;

	public H2Mixin() {
		this.msm = null;
		this.logger = null;
		this.connId = 0;
		this.dbConnection = null;
		this.tables = null;
	}
	public H2Mixin(MusicSqlManager msm, String url, Connection conn, Properties info) {
		this.msm = msm;
		//this.logger = Logger.getLogger(this.getClass());
		this.connId = generateConnID(conn);
		this.dbConnection = conn;
		this.tables = new HashMap<String, TableInfo>();
	}
	protected int generateConnID(Connection conn) {
		return connindex++;
	}
	/**
	 * Get the name of this DBnterface mixin object.
	 * @return the name
	 */
	@Override
	public String getMixinName() {
		return MIXIN_NAME;
	}
	/**
	 * Do what is needed to close down the database connection.
	 */
	@Override
	public void close() {
		// nothing yet
	}
	public String getDatabaseName() {
		logger.error(EELFLoggerDelegate.errorLogger, "H2Mixin getDatabaseName: not implemented");
		return "mdbc";
	}
	/**
	 * Get a set of the table names in the database. The table names should be returned in UPPER CASE.
	 * @return the set
	 */
	@Override
	public Set<String> getSQLTableSet() {
		Set<String> set = new TreeSet<String>();
		try {
			Statement stmt = dbConnection.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'TABLE'");
			while (rs.next()) {
				String s = rs.getString("TABLE_NAME").toUpperCase();
				set.add(s);
			}
			stmt.close();
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"getSQLTableSet: "+e);
		}
		return set;
	}
	/**
	 * Return a TableInfo object for the specified table.
	 * This method first looks in a cache of previously constructed TableInfo objects for the table.
	 * If not found, it queries the INFORMATION_SCHEMA.COLUMNS table to obtain the column names and types of the table,
	 * and the INFORMATION_SCHEMA.INDEXES table to find the primary key columns.  It creates a new TableInfo object with
	 * the results.
	 * @param tableName the table to look up
	 * @return a TableInfo object containing the info we need, or null if the table does not exist
	 */
	@Override
	public TableInfo getTableInfo(String tableName) {
		TableInfo ti = tables.get(tableName);
		if (ti == null) {
			try {
				String tbl = tableName.toUpperCase();
				String sql = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='"+tbl+"'";
				ResultSet rs = executeSQLRead(sql);
				if (rs != null) {
					ti = new TableInfo();
					while (rs.next()) {
						String name = rs.getString("COLUMN_NAME");
						int type = rs.getInt("DATA_TYPE");
						ti.columns.add(name);
						ti.coltype.add(type);
						ti.iskey.add(false);
					}
					rs.getStatement().close();
					sql = "SELECT COLUMN_NAME, PRIMARY_KEY FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME='"+tbl+"'";
					rs = executeSQLRead(sql);
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
					logger.error(EELFLoggerDelegate.errorLogger,"Cannot retrieve table info for table "+tableName+" from H2.");
					
				}
			} catch (SQLException e) {
				logger.error(EELFLoggerDelegate.errorLogger,"Cannot retrieve table info for table "+tableName+" from H2: "+e);
				
				return null;
			}
			tables.put(tableName, ti);
		}
		return ti;
	}
	/**
	 * This method should create triggers on the database for each row after every INSERT,
	 * UPDATE and DELETE, and before every SELECT.
	 * @param tableName this is the table on which triggers are being created.
	 */
	@Override
	public void createSQLTriggers(String tableName) {
		try {
			// Give the triggers a way to find this MSM
			for (String name : getTriggerNames(tableName)) {
				logger.info(EELFLoggerDelegate.applicationLogger,"ADD trigger "+name+" to msm_map");
				msm.register(name);
			}
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS I_"+connId+"_" +tableName+" AFTER INSERT ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS U_"+connId+"_" +tableName+" AFTER UPDATE ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS D_"+connId+"_" +tableName+" AFTER DELETE ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS S_"+connId+"_" +tableName+" BEFORE SELECT ON "+tableName+" CALL \""+triggerClassName+"\"");
			dbConnection.commit();
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"createSQLTriggers: "+e);
			
		}
	}
	/**
	 * This method should drop all triggers previously created in the database for the table.
	 * @param tableName this is the table on which triggers are being dropped.
	 */
	@Override
	public void dropSQLTriggers(String tableName) {
		try {
			for (String name : getTriggerNames(tableName)) {
				logger.info(EELFLoggerDelegate.applicationLogger,"REMOVE trigger "+name+" from msmmap");
				
				executeSQLWrite("DROP TRIGGER IF EXISTS " +name);
				msm.unregister(name);
			}
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"dropSQLTriggers: "+e);
			
		}
	}
	private String[] getTriggerNames(String tableName) {
		return new String[] {
			"I_" + connId + "_" + tableName,	// INSERT trigger
			"U_" + connId + "_" + tableName,	// UPDATE trigger
			"D_" + connId + "_" + tableName,	// DELETE trigger
			"S_" + connId + "_" + tableName		// SELECT trigger
		};
	}
	/**
	 * This method deletes a row from the SQL database, defined by the map passed as an argument.
	 * @param tableName the table to delete the row from
	 * @param map map of column names &rarr; values to use for the keys when deleting the row
	 */
	@Override
	public void deleteRowFromSqlDb(String tableName, Map<String, Object> map) {
		TableInfo ti = getTableInfo(tableName);
		StringBuilder where = new StringBuilder();
		String pfx = "";
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				String col = ti.columns.get(i);
				Object val = map.get(col);
				where.append(pfx).append(col).append("= $$").append(val.toString()).append("$$");
				pfx = " AND ";
			}
		}
		try {
			String sql = String.format("DELETE FROM %s WHERE %s", tableName, where.toString());
			executeSQLWrite(sql);
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"deleteRowFromSqlDb: "+e);
			e.printStackTrace();
		}
	}

	@Override
	public void insertRowIntoSqlDb(String tableName, Map<String, Object> map) {
		// First construct the value string and column name string for the db write
		TableInfo ti = getTableInfo(tableName);
		StringBuilder fields = new StringBuilder();
		StringBuilder values = new StringBuilder();
		String pfx = "";
		for (String col : ti.columns) {
			fields.append(pfx).append(col);
			values.append(pfx).append(Utils.getStringValue(map.get(col)));
			pfx = ", ";
		}

		try {
			String sql = String.format("INSERT INTO %s (%s) VALUES (%s);", tableName, fields.toString(), values.toString());
			executeSQLWrite(sql);
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"insertRowIntoSqlDb: "+e);
			// TODO - rewrite this UPDATE command should not update key fields
			StringBuilder where = new StringBuilder();
			pfx = "";
			for (int i = 0; i < ti.columns.size(); i++) {
				if (ti.iskey.get(i)) {
					String col = ti.columns.get(i);
					Object val = map.get(col);
					where.append(pfx).append(col).append("= $$").append(val.toString()).append("$$");
					pfx = " AND ";
				}
			}
			String sql = String.format("UPDATE %s SET (%s) = (%s) WHERE %s", tableName, fields.toString(), values.toString(), where.toString());
			try {
				executeSQLWrite(sql);
			} catch (SQLException e1) {
				logger.error(EELFLoggerDelegate.errorLogger,"insertRowIntoSqlDb: "+e);
				e1.printStackTrace();
			}
		}
	}

	/**
	 * This method executes a read query in the SQL database.  Methods that call this method should be sure
	 * to call resultset.getStatement().close() when done in order to free up resources.
	 * @param sql the query to run
	 * @return a ResultSet containing the rows returned from the query
	 */
	@Override
	public ResultSet executeSQLRead(String sql) {
		logger.debug("Executing SQL read:"+ sql);
		ResultSet rs = null;
		try {
			Statement stmt = dbConnection.createStatement();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"executeSQLRead: "+e);
		}
		return rs;
	}

	/**
	 * This method executes a write query in the sql database.
	 * @param sql the SQL to be sent to H2
	 * @throws SQLException if an underlying JDBC method throws an exception
	 */
	protected void executeSQLWrite(String sql) throws SQLException {
		logger.info(EELFLoggerDelegate.applicationLogger,"Executing SQL write:"+ sql);
		
//		dbConnection.setAutoCommit(false);
		Statement stmt = dbConnection.createStatement();
		stmt.execute(sql);
		stmt.close();
//		dbConnection.commit();
	}
	/**
	 * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
	 * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
	 * @param sql the SQL statement that is about to be executed
	 * @return keys of rows that will be 
	 */
	@Override
	public void preStatementHook(final String sql) {
		// do nothing
	}
	/**
	 * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
	 * statement actions can be copied back to Cassandra/MUSIC.
	 * @param sql the SQL statement that was executed
	 */
	@Override
	public void postStatementHook(final String sql) {
		// do nothing
	}
	@Override
	public void synchronizeData(String tableName) {
		// TODO Auto-generated method stub
		
	}
	
	/**
	 * Return a list of "reserved" names, that should not be used by MySQL client/MUSIC
	 * These are reserved for mdbc
	 */
	@Override
	public List<String> getReservedTblNames() {
		ArrayList<String> rsvdTables = new ArrayList<String>();
		//Add table names here as necessary
		return rsvdTables;
	}
	@Override
	public String getPrimaryKey(String sql, String tableName) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
