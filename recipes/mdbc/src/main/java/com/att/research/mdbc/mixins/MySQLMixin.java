package com.att.research.mdbc.mixins;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.att.research.mdbc.MusicSqlManager;
import com.att.research.mdbc.TableInfo;

/**
 * This class provides the methods that MDBC needs in order to mirror data to/from a
 * <a href="https://dev.mysql.com/">MySQL</a> or <a href="http://mariadb.org/">MariaDB</a> database instance.
 * This class uses the <code>JSON_OBJECT()</code> database function, which means it requires the following
 * minimum versions of either database:
 * <table summary="">
 * <tr><th>DATABASE</th><th>VERSION</th></tr>
 * <tr><td>MySQL</td><td>5.7.8</td></tr>
 * <tr><td>MariaDB</td><td>10.2.3 (Note: 10.2.3 is currently (July 2017) a <i>beta</i> release)</td></tr>
 * </table>
 *
 * @author Robert P. Eby
 */
public class MySQLMixin implements DBInterface {
	public static final String MIXIN_NAME = "mysql";
	public static final String TRANS_TBL = "MDBC_TRANSLOG";
	private static final String CREATE_TBL_SQL =
		"CREATE TABLE IF NOT EXISTS "+TRANS_TBL+
		" (IX INT AUTO_INCREMENT, CONNID INT, OP CHAR(1), TABLENAME VARCHAR(255), KEYDATA VARCHAR(512), PRIMARY KEY (IX))";

	private final MusicSqlManager msm;
	private final Logger logger;
	private final int connId;
	private final Connection dbConnection;
	private final Map<String, TableInfo> tables;
	private boolean server_tbl_created = false;

	public MySQLMixin() {
		this.msm = null;
		this.logger = null;
		this.connId = 0;
		this.dbConnection = null;
		this.tables = null;
	}
	public MySQLMixin(MusicSqlManager msm, String url, Connection conn, Properties info) {
		this.msm = msm;
		this.logger = Logger.getLogger(this.getClass());
		this.connId = generateConnID(conn);
		this.dbConnection = conn;
		this.tables = new HashMap<String, TableInfo>();
	}
	// This is used to generate a unique connId for this connection to the DB.
	private int generateConnID(Connection conn) {
		int rv = (int) System.currentTimeMillis();	// random-ish
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT CONNECTION_ID() AS IX");
			if (rs.next()) {
				rv = rs.getInt("IX");
			}
			stmt.close();
		} catch (SQLException e) {
			logger.error("generateConnID: problem generating a connection ID!");
			logger.error(e);
		}
		return rv;
	}

	/**
	 * Get the name of this DBnterface mixin object.
	 * @return the name
	 */
	@Override
	public String getMixinName() {
		return MIXIN_NAME;
	}

	@Override
	public void close() {
		// nothing yet
	}

	/**
	 * Get a set of the table names in the database. The table names should be returned in UPPER CASE.
	 * @return the set
	 */
	@Override
	public Set<String> getSQLTableSet() {
		Set<String> set = new TreeSet<String>();
		String sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_TYPE='BASE TABLE'";
		try {
			Statement stmt = dbConnection.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				String s = rs.getString("TABLE_NAME").toUpperCase();
				set.add(s);
			}
			stmt.close();
		} catch (SQLException e) {
			logger.warn("getSQLTableSet: "+e);
		}
		return set;
	}
/*
mysql> describe tables;
+-----------------+---------------------+------+-----+---------+-------+
| Field           | Type                | Null | Key | Default | Extra |
+-----------------+---------------------+------+-----+---------+-------+
| TABLE_CATALOG   | varchar(512)        | NO   |     |         |       |
| TABLE_SCHEMA    | varchar(64)         | NO   |     |         |       |
| TABLE_NAME      | varchar(64)         | NO   |     |         |       |
| TABLE_TYPE      | varchar(64)         | NO   |     |         |       |
| ENGINE          | varchar(64)         | YES  |     | NULL    |       |
| VERSION         | bigint(21) unsigned | YES  |     | NULL    |       |
| ROW_FORMAT      | varchar(10)         | YES  |     | NULL    |       |
| TABLE_ROWS      | bigint(21) unsigned | YES  |     | NULL    |       |
| AVG_ROW_LENGTH  | bigint(21) unsigned | YES  |     | NULL    |       |
| DATA_LENGTH     | bigint(21) unsigned | YES  |     | NULL    |       |
| MAX_DATA_LENGTH | bigint(21) unsigned | YES  |     | NULL    |       |
| INDEX_LENGTH    | bigint(21) unsigned | YES  |     | NULL    |       |
| DATA_FREE       | bigint(21) unsigned | YES  |     | NULL    |       |
| AUTO_INCREMENT  | bigint(21) unsigned | YES  |     | NULL    |       |
| CREATE_TIME     | datetime            | YES  |     | NULL    |       |
| UPDATE_TIME     | datetime            | YES  |     | NULL    |       |
| CHECK_TIME      | datetime            | YES  |     | NULL    |       |
| TABLE_COLLATION | varchar(32)         | YES  |     | NULL    |       |
| CHECKSUM        | bigint(21) unsigned | YES  |     | NULL    |       |
| CREATE_OPTIONS  | varchar(255)        | YES  |     | NULL    |       |
| TABLE_COMMENT   | varchar(2048)       | NO   |     |         |       |
+-----------------+---------------------+------+-----+---------+-------+
 */
	/**
	 * Return a TableInfo object for the specified table.
	 * This method first looks in a cache of previously constructed TableInfo objects for the table.
	 * If not found, it queries the INFORMATION_SCHEMA.COLUMNS table to obtain the column names, types, and indexes of the table.
	 * It creates a new TableInfo object with the results.
	 * @param tableName the table to look up
	 * @return a TableInfo object containing the info we need, or null if the table does not exist
	 */
	@Override
	public TableInfo getTableInfo(String tableName) {
		TableInfo ti = tables.get(tableName);
		if (ti == null) {
			try {
				String tbl = tableName.toUpperCase();
				String sql = "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_KEY FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='"+tbl+"'";
				ResultSet rs = executeSQLRead(sql);
				if (rs != null) {
					ti = new TableInfo();
					while (rs.next()) {
						String name = rs.getString("COLUMN_NAME");
						String type = rs.getString("DATA_TYPE");
						String ckey = rs.getString("COLUMN_KEY");
						ti.columns.add(name);
						ti.coltype.add(mapDatatypeNameToType(type));
						ti.iskey.add(ckey != null && !ckey.equals(""));
					}
					rs.getStatement().close();
				} else {
					logger.warn("Cannot retrieve table info for table "+tableName+" from MySQL.");
				}
			} catch (SQLException e) {
				logger.warn("Cannot retrieve table info for table "+tableName+" from MySQL: "+e);
				return null;
			}
			tables.put(tableName, ti);
		}
		return ti;
	}
	// Map MySQL data type names to the java.sql.Types equivalent
	private int mapDatatypeNameToType(String nm) {
		switch (nm) {
		case "tinyint":		return Types.TINYINT;
		case "smallint":	return Types.SMALLINT;
		case "mediumint":
		case "int":			return Types.INTEGER;
		case "bigint":		return Types.BIGINT;
		case "decimal":
		case "numeric":		return Types.DECIMAL;
		case "float":		return Types.FLOAT;
		case "double":		return Types.DOUBLE;
		case "datetime":	return Types.DATE;
		case "time":		return Types.TIME;
		case "timestamp":	return Types.TIMESTAMP;
		case "char":		return Types.CHAR;
		case "varchar":		return Types.VARCHAR;
		case "blob":		return Types.BLOB;
		default:
			logger.error("unrecognized and/or unsupported data type "+nm);
			return Types.VARCHAR;
		}
	}
	@Override
	public void createSQLTriggers(String tableName) {
		// Don't create triggers for the table the triggers write into!!!
		if (tableName.equals(TRANS_TBL))
			return;
		try {
			if (!server_tbl_created) {
				try {
					Statement stmt = dbConnection.createStatement();
					stmt.execute(CREATE_TBL_SQL);
					stmt.close();
					logger.debug("createSQLTriggers: Server side dirty table created.");
					server_tbl_created = true;
				} catch (SQLException e) {
					logger.error("createSQLTriggers: problem creating the "+TRANS_TBL+" table!");
					logger.error(e);
				}
			}

			// Give the triggers a way to find this MSM
			for (String name : getTriggerNames(tableName)) {
				logger.debug("ADD trigger "+name+" to msm_map");
				msm.register(name);
			}
			// No SELECT trigger
			executeSQLWrite(generateTrigger(tableName, "INSERT"));
			executeSQLWrite(generateTrigger(tableName, "UPDATE"));
			executeSQLWrite(generateTrigger(tableName, "DELETE"));
		} catch (SQLException e) {
			logger.warn("createSQLTriggers: "+e);
		}
	}
/*
CREATE TRIGGER `triggername` BEFORE UPDATE ON `table`
FOR EACH ROW BEGIN
INSERT INTO `log_table` ( `field1` `field2`, ...) VALUES ( NEW.`field1`, NEW.`field2`, ...) ;
END;

OLD.field refers to the old value
NEW.field refers to the new value
*/
	private String generateTrigger(String tableName, String op) {
		boolean isdelete = op.equals("DELETE");
		TableInfo ti = getTableInfo(tableName);
		StringBuilder json = new StringBuilder("JSON_OBJECT(");		// JSON_OBJECT(key, val, key, val) page 1766
		String pfx = "";
		for (String col : ti.columns) {
// Note: we could just store the keys in this table, but then we would need to refetch the row
// and propogate it to the MusicInterface
//			if (ti.iskey(col)) {
				json.append(pfx)
					.append("'").append(col).append("', ")
					.append(isdelete ? "OLD." : "NEW.")
					.append(col);
				pfx = ", ";
//			}
		}
		json.append(")");
		StringBuilder sb = new StringBuilder()
		  .append("CREATE TRIGGER ")		// IF NOT EXISTS not supported by MySQL!
		  .append(String.format("%s_%d_%s", op.substring(0, 1), connId, tableName))
		  .append(" AFTER ")
		  .append(op)
		  .append(" ON ")
		  .append(tableName)
		  .append(" FOR EACH ROW INSERT INTO ")
		  .append(TRANS_TBL)
		  .append(" (TABLENAME, CONNID, OP, KEYDATA) VALUES('")
		  .append(tableName)
		  .append("', ")
		  .append(connId)
		  .append(", ")
		  .append(isdelete ? "'D'" : (op.equals("INSERT") ? "'I'" : "'U'"))
		  .append(", ")
		  .append(json.toString())
		  .append(")");
		return sb.toString();
	}
	private String[] getTriggerNames(String tableName) {
		return new String[] {
			"I_" + connId + "_" + tableName,	// INSERT trigger
			"U_" + connId + "_" + tableName,	// UPDATE trigger
			"D_" + connId + "_" + tableName		// DELETE trigger
		};
	}

	@Override
	public void dropSQLTriggers(String tableName) {
		try {
			for (String name : getTriggerNames(tableName)) {
				logger.debug("REMOVE trigger "+name+" from msmmap");
				executeSQLWrite("DROP TRIGGER IF EXISTS " +name);
				msm.unregister(name);
			}
		} catch (SQLException e) {
			logger.warn("dropSQLTriggers: "+e);
		}
	}

	@Override
	public void insertRowIntoSqlDb(String tableName, Map<String, Object> map) {
		TableInfo ti = getTableInfo(tableName);
		String sql = "";
		if (rowExists(tableName, ti, map)) {
			// Update - Construct the what and where strings for the DB write
			StringBuilder what  = new StringBuilder();
			StringBuilder where = new StringBuilder();
			String pfx = "";
			String pfx2 = "";
			for (int i = 0; i < ti.columns.size(); i++) {
				String col = ti.columns.get(i);
				String val = Utils.getStringValue(map.get(col));
				if (ti.iskey.get(i)) {
					where.append(pfx).append(col).append("=").append(val);
					pfx = " AND ";
				} else {
					what.append(pfx2).append(col).append("=").append(val);
					pfx2 = ", ";
				}
			}
			sql = String.format("UPDATE %s SET %s WHERE %s", tableName, what.toString(), where.toString());
		} else {
			// Construct the value string and column name string for the DB write
			StringBuilder fields = new StringBuilder();
			StringBuilder values = new StringBuilder();
			String pfx = "";
			for (String col : ti.columns) {
				fields.append(pfx).append(col);
				values.append(pfx).append(Utils.getStringValue(map.get(col)));
				pfx = ", ";
			}
			sql = String.format("INSERT INTO %s (%s) VALUES (%s);", tableName, fields.toString(), values.toString());
		}
		try {
			executeSQLWrite(sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		// TODO - remove any entries from MDBC_TRANSLOG corresponding to this update
		//	SELECT IX, OP, KEYDATA FROM MDBC_TRANS_TBL WHERE CONNID = "+connId AND TABLENAME = tblname
	}
	private boolean rowExists(String tableName, TableInfo ti, Map<String, Object> map) {
		StringBuilder where = new StringBuilder();
		String pfx = "";
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				String col = ti.columns.get(i);
				String val = Utils.getStringValue(map.get(col));
				where.append(pfx).append(col).append("=").append(val);
				pfx = " AND ";
			}
		}
		String sql = String.format("SELECT * FROM %s WHERE %s", tableName, where.toString());
		ResultSet rs = executeSQLRead(sql);
		try {
			boolean rv = rs.next();
			rs.close();
			return rv;
		} catch (SQLException e) {
			return false;
		}
	}
	public void insertRowIntoSqlDbOLD(String tableName, Map<String, Object> map) {
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
			logger.debug("Insert failed because row exists, do an update");
			StringBuilder where = new StringBuilder();
			pfx = "";
			String pfx2 = "";
			fields.setLength(0);
			for (int i = 0; i < ti.columns.size(); i++) {
				String col = ti.columns.get(i);
				String val = Utils.getStringValue(map.get(col));
				if (ti.iskey.get(i)) {
					where.append(pfx).append(col).append("=").append(val);
					pfx = " AND ";
				} else {
					fields.append(pfx2).append(col).append("=").append(val);
					pfx2 = ", ";
				}
			}
			String sql = String.format("UPDATE %s SET %s WHERE %s", tableName, fields.toString(), where.toString());
			try {
				executeSQLWrite(sql);
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
		}
	}

	@Override
	public void deleteRowFromSqlDb(String tableName, Map<String, Object> map) {
		TableInfo ti = getTableInfo(tableName);
		StringBuilder where = new StringBuilder();
		String pfx = "";
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				String col = ti.columns.get(i);
				Object val = map.get(col);
				where.append(pfx).append(col).append("=").append(Utils.getStringValue(val));
				pfx = " AND ";
			}
		}
		try {
			String sql = String.format("DELETE FROM %s WHERE %s", tableName, where.toString());
			executeSQLWrite(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method executes a read query in the SQL database.  Methods that call this method should be sure
	 * to call resultset.getStatement().close() when done in order to free up resources.
	 * @param sql the query to run
	 * @return a ResultSet containing the rows returned from the query
	 */
	protected ResultSet executeSQLRead(String sql) {
		logger.debug("Executing SQL read:"+ sql);
		ResultSet rs = null;
		try {
			Statement stmt = dbConnection.createStatement();
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			logger.warn("executeSQLRead: "+e);
		}
		return rs;
	}

	/**
	 * This method executes a write query in the sql database.
	 * @param sql the SQL to be sent to MySQL
	 * @throws SQLException if an underlying JDBC method throws an exception
	 */
	protected void executeSQLWrite(String sql) throws SQLException {
		logger.debug("Executing SQL write:"+ sql);
		Statement stmt = dbConnection.createStatement();
		stmt.execute(sql);
		stmt.close();
	}

	/**
	 * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
	 * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
	 * @param sql the SQL statement that is about to be executed
	 */
	@Override
	public void preStatementHook(final String sql) {
		if (sql != null && sql.trim().toLowerCase().startsWith("select")) {
			String[] parts = sql.trim().split(" ");
			Set<String> set = getSQLTableSet();
			for (String part : parts) {
				if (set.contains(part.toUpperCase())) {
					// Found a candidate table name in the SELECT SQL -- update this table
					msm.readDirtyRowsAndUpdateDb(part);
				}
			}
		}
	}
	/**
	 * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
	 * statement actions can be copied back to Cassandra/MUSIC.
	 * @param sql the SQL statement that was executed
	 */
	@Override
	public void postStatementHook(final String sql) {
		if (sql != null) {
			String[] parts = sql.trim().split(" ");
			String cmd = parts[0].toLowerCase();
			if ("delete".equals(cmd) || "insert".equals(cmd) || "update".equals(cmd)) {
				// copy from DB.MDBC_TRANSLOG where connid == myconnid
				// then delete from MDBC_TRANSLOG
				String sql2 = "SELECT IX, TABLENAME, OP, KEYDATA FROM "+TRANS_TBL+" WHERE CONNID = "+connId;
				try {
					ResultSet rs = executeSQLRead(sql2);
					Set<Integer> rows = new TreeSet<Integer>();
					while (rs.next()) {
						int ix      = rs.getInt("IX");
						String op   = rs.getString("OP");
						String tbl  = rs.getString("TABLENAME");
						String keys = rs.getString("KEYDATA");
						JSONObject jo = new JSONObject(new JSONTokener(keys));
						Object[] row = jsonToRow(tbl, jo);
						// copy to cassandra
						if (op.startsWith("D")) {
							msm.deleteFromEntityTableInMusic(tbl, row);
						} else {
							msm.updateDirtyRowAndEntityTableInMusic(tbl, row);
						}
						rows.add(ix);
					}
					if (rows.size() > 0) {
						sql2 = "DELETE FROM "+TRANS_TBL+" WHERE IX = ?";
						PreparedStatement ps = dbConnection.prepareStatement(sql2);
						logger.debug("Executing: "+sql2);
						logger.debug("  For ix = "+rows);
						for (int ix : rows) {
							ps.setInt(1, ix);
							ps.execute();
						}
						ps.close();
					}
				} catch (SQLException e) {
					logger.warn("Exception in postStatementHook: "+e);
					e.printStackTrace();
				}
			}
		}
	}
	@SuppressWarnings("deprecation")
	private Object[] jsonToRow(String tbl, JSONObject jo) {
		TableInfo ti = getTableInfo(tbl);
		Object[] rv = new Object[ti.columns.size()];
		for (int i = 0; i < rv.length; i++) {
			String colname = ti.columns.get(i);
			switch (ti.coltype.get(i)) {
			case Types.BIGINT:
				rv[i] = jo.optLong(colname, 0);
				break;
			case Types.BOOLEAN:
				rv[i] = jo.optBoolean(colname, false);
				break;
			case Types.BLOB:
				logger.error("WE DO NOT SUPPORT BLOBS IN H2!! COLUMN NAME="+colname);
				// throw an exception here???
				break;
			case Types.DOUBLE:
				rv[i] = jo.optDouble(colname, 0);
				break;
			case Types.INTEGER:
				rv[i] = jo.optInt(colname, 0);
				break;
			case Types.TIMESTAMP:
				rv[i] = new Date(jo.optString(colname, ""));
				break;
			case Types.VARCHAR:
			default:
				rv[i] = jo.optString(colname, "");
				break;
			}
		}
		return rv;
	}
}
