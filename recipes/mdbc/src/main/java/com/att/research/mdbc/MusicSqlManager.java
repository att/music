package com.att.research.mdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import com.att.research.mdbc.mixins.DBInterface;
import com.att.research.mdbc.mixins.MixinFactory;
import com.att.research.mdbc.mixins.MusicInterface;
import com.att.research.mdbc.mixins.MusicMixin;
import com.att.research.mdbc.mixins.Utils;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import com.att.research.exceptions.MDBCServiceException;
import com.att.research.exceptions.QueryException;
import com.att.research.logging.*;
import com.att.research.logging.format.AppMessages;
import com.att.research.logging.format.ErrorSeverity;
import com.att.research.logging.format.ErrorTypes;

/**
* <p>
* MUSIC SQL Manager - code that helps take data written to a SQL database and seamlessly integrates it
* with <a href="https://github.com/att/music">MUSIC</a> that maintains data in a No-SQL data-store
* (<a href="http://cassandra.apache.org/">Cassandra</a>) and protects access to it with a distributed
* locking service (based on <a href="https://zookeeper.apache.org/">Zookeeper</a>).
* </p>
* <p>
* This code will support transactions by taking note of the value of the autoCommit flag, and of calls
* to <code>commit()</code> and <code>rollback()</code>.  These calls should be made by the user's JDBC
* client.
* </p>
*
* @author  Bharath Balasubramanian, Robert Eby
*/
public class MusicSqlManager {
	/** The property name to use to enable/disable the MusicSqlManager entirely. */
	public static final String KEY_DISABLED         = "disabled";
	/** The property name to use to select the DB 'mixin'. */
	public static final String KEY_DB_MIXIN_NAME    = "MDBC_DB_MIXIN";
	/** The property name to use to select the MUSIC 'mixin'. */
	public static final String KEY_MUSIC_MIXIN_NAME = "MDBC_MUSIC_MIXIN";
	/** The name of the default mixin to use for the DBInterface. */
	public static final String DB_MIXIN_DEFAULT     = "mysql";//"h2";
	/** The name of the default mixin to use for the MusicInterface. */
	public static final String MUSIC_MIXIN_DEFAULT  = "cassandra2";//"cassandra2";

	private static final Map<String, MusicSqlManager> msm_map = new Hashtable<String, MusicSqlManager>(); // needs to be synchronized

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicSqlManager.class);
	
	/**
	 * Get the MusicSqlManager instance that applies to a specific trigger in a specific database connection.
	 * @param key the key used to fetch the MusicSqlManager from the map.  The key consists of three parts:
	 * 	<pre>triggertype_id_tablename</pre>
	 * where triggertype is I/D/S/U (for insert/delete/select/update), ID is the unique ID assigned to this
	 * instance of MusicSqlManager, and tablename is the name of the table the trigger applies to.
	 * @return the corresponding MusicSqlManager
	 */
	public static MusicSqlManager getMusicSqlManager(String key) {
		return msm_map.get(key);
	}
	/**
	 * Get a new instance of MusicSqlManager for a specific JDBC URL.
	 * @param url the URL to look for
	 * @param c the JDBC Connection
	 * @param info the JDBC Properties
	 * @return the corresponding MusicSqlManager, or null if no proxy is needed for this URL
	 * @throws MDBCServiceException 
	 */
	public static MusicSqlManager getMusicSqlManager(String url, Connection c, Properties info) {
		String s = info.getProperty(KEY_DISABLED, "false");
		if (s.equalsIgnoreCase("true")) {
			return null;
		}

		// To support transactions we need one MusicSqlManager per Connection
		try {
			return new MusicSqlManager(url, c, info);
		} catch (MDBCServiceException e) {
			logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
		}
		return null;

//		synchronized (msm_map) {
//			MusicSqlManager mgr = msm_map.get(url);
//			if (mgr == null) {
//				mgr = new MusicSqlManager(url, c, info);
//				msm_map.put(url, mgr);
//			}
//			return mgr;
//		}
	}

	
	private final DBInterface dbi;
	private final MusicInterface mi;
	private final Set<String> table_set;
	private boolean autocommit;			// a copy of the autocommit flag from the JDBC Connection

	/**
	 * Build a MusicSqlManager for a DB connection.  This construct may only be called by getMusicSqlManager(),
	 * which will ensure that only one MusicSqlManager is created per URL.
	 * This is the location where the appropriate mixins to use for the MusicSqlManager should be determined.
	 * They should be picked based upon the URL and the properties passed to this constructor.
	 * <p>
	 * At the present time, we only support the use of the H2Mixin (for access to a local H2 database),
	 * with the CassandraMixin (for direct access to a Cassandra noSQL DB as the persistence layer).
	 * </p>
	 *
	 * @param url the JDBC URL which was used to connection to the database
	 * @param conn the actual connection to the database
	 * @param info properties passed from the initial JDBC connect() call
	 * @throws MDBCServiceException 
	 */
	private MusicSqlManager(String url, Connection conn, Properties info) throws MDBCServiceException {
		try {
			info.putAll(Utils.getMdbcProperties());
			String mixin1  = info.getProperty(KEY_DB_MIXIN_NAME, DB_MIXIN_DEFAULT);
			String mixin2  = info.getProperty(KEY_MUSIC_MIXIN_NAME, MUSIC_MIXIN_DEFAULT);
			this.dbi       = MixinFactory.createDBInterface(mixin1, this, url, conn, info);
			this.mi        = MixinFactory.createMusicInterface(mixin2, this, dbi, url, info);
			this.table_set = Collections.synchronizedSet(new HashSet<String>());
			this.autocommit = true;
			this.mi.createKeyspace();
			MusicMixin.loadProperties();
		}catch(Exception e) {
			throw new MDBCServiceException(e.getMessage());
		}
	}

	public void setAutoCommit(boolean b) {
		if (b != autocommit) {
			autocommit = b;
			logger.info(EELFLoggerDelegate.applicationLogger,"autocommit changed to "+b);
			if (b) {
				// My reading is that turning autoCOmmit ON should automatically commit any outstanding transaction
				commit();
			}
		}
	}

	/**
	 * Register this MusicSqlManager under a name, to be retrived later by a different thread.
	 * @param name the name to use
	 */
	public void register(String name) {
		msm_map.put(name, this);
	}
	/**
	 * Unregister this MusicSqlManager under a name.
	 * @param name the name to use
	 */
	public void unregister(String name) {
		msm_map.remove(name);
	}
	/**
	 * Close this MusicSqlManager.
	 */
	public void close() {
		// remove from msm_map
		for (String key : new TreeSet<String>(msm_map.keySet())) {
			if (msm_map.get(key) == this) {
				msm_map.remove(key);
				break;
			}
		}
		if (dbi != null) {
			dbi.close();
		}
		if (mi != null) {
			mi.close();
		}
	}

	/**
	 * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
	 * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
	 * @param sql the SQL statement that is about to be executed
	 */
	public void preStatementHook(final String sql) {
		dbi.preStatementHook(sql);
	}
	/**
	 * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
	 * statement actions can be copied back to Cassandra/MUSIC.
	 * @param sql the SQL statement that was executed
	 * @param keys that were updated in the sql call
	 */
	public void postStatementHook(final String sql) {
		dbi.postStatementHook(sql);
	}
	/**
	 * Synchronize the list of tables in H2 with the list in MUSIC. This function should be called when the
	 * proxy first starts, and whenever there is the possibility that tables were created or dropped.  It is synchronized
	 * in order to prevent multiple threads from running this code in parallel.
	 */
	public synchronized void synchronizeTables() throws QueryException {
			Set<String> set1 = dbi.getSQLTableSet();	// set of tables in the database
			logger.info(EELFLoggerDelegate.applicationLogger, "synchronizing tables:" + set1);
			for (String tableName : set1) {
				// This map will be filled in if this table was previously discovered
				if (!table_set.contains(tableName) && !dbi.getReservedTblNames().contains(tableName)) {
					logger.info(EELFLoggerDelegate.applicationLogger, "New table discovered: "+tableName);
					try {
						mi.initializeMusicForTable(tableName);
						mi.createDirtyRowTable(tableName);
						dbi.createSQLTriggers(tableName);
						table_set.add(tableName);
						synchronizeTableData(tableName);
						logger.info(EELFLoggerDelegate.applicationLogger, "synchronized tables:" +
									table_set.size() + "/" + set1.size() + "tables uploaded");
					} catch (Exception e) {
						logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(),AppMessages.UNKNOWNERROR, ErrorSeverity.CRITICAL, ErrorTypes.QUERYERROR);
						//logger.error(EELFLoggerDelegate.errorLogger, "Exception synchronizeTables: "+e);
						throw new QueryException();
					}
				}
			}

//			Set<String> set2 = getMusicTableSet(music_ns);
			// not working - fix later
//			for (String tbl : set2) {
//				if (!set1.contains(tbl)) {
//					logger.debug("Old table dropped: "+tbl);
//					dropSQLTriggers(tbl, conn);
//					// ZZTODO drop camunda table ?
//				}
//			}
	}

	/**
	 * On startup, copy dirty data from Cassandra to H2. May not be needed.
	 * @param tableName 
	 */
	public void synchronizeTableData(String tableName) {
		// TODO - copy MUSIC -> H2
		dbi.synchronizeData(tableName);
	}
	/**
	 * This method is called whenever there is a SELECT on a local SQL table, and should be called by the underlying databases
	 * triggering mechanism.  It first checks the local dirty bits table to see if there are any keys in Cassandra whose value
	 * has not yet been sent to SQL.  If there are, the appropriate values are copied from Cassandra to the local database.
	 * @param tableName This is the table on which the SELECT is being performed
	 */
	public void readDirtyRowsAndUpdateDb(String tableName) {
		mi.readDirtyRowsAndUpdateDb(tableName);
	}
	/**
	 * This method is called whenever there is an INSERT or UPDATE to a local SQL table, and should be called by the underlying databases
	 * triggering mechanism. It updates the MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write.
	 * Music propagates it to the other replicas.  If the local database is in the middle of a transaction, the updates to MUSIC are
	 * delayed until the transaction is either committed or rolled back.
	 *
	 * @param tableName This is the table that has changed.
	 * @param changedRow This is information about the row that has changed, an array of objects representing the data being inserted/updated
	 */
	public void updateDirtyRowAndEntityTableInMusic(String tableName, Object[] changedRow) {
		//TODO: is this right? should we be saving updates at the client? we should leverage jdbc to handle this
		if (autocommit) {
			mi.updateDirtyRowAndEntityTableInMusic(tableName, changedRow);
		} else {
			saveUpdate("update", tableName, changedRow);
		}
	}
	/**
	 * This method is called whenever there is a DELETE on a local SQL table, and should be called by the underlying databases
	 * triggering mechanism. It updates the MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL DELETE.
	 * Music propagates it to the other replicas.  If the local database is in the middle of a transaction, the DELETEs to MUSIC are
	 * delayed until the transaction is either committed or rolled back.
	 * @param tableName This is the table on which the select is being performed
	 * @param oldRow This is information about the row that is being deleted
	 */
	public void deleteFromEntityTableInMusic(String tableName, Object[] oldRow) {
		if (autocommit) {
			mi.deleteFromEntityTableInMusic(tableName, oldRow);
		} else {
			saveUpdate("delete", tableName, oldRow);
		}
	}
	
	/**
	 * Returns all keys that matches the current sql statement, and not in already updated keys.
	 * 
	 * @param sql the query that we are getting keys for
	 * @deprecated
	 */
	public ArrayList<String> getMusicKeys(String sql) {
		ArrayList<String> musicKeys = new ArrayList<String>();
		/*
		try {
			net.sf.jsqlparser.statement.Statement stmt = CCJSqlParserUtil.parse(sql);
			if (stmt instanceof Insert) {
				Insert s = (Insert) stmt;
				String tbl = s.getTable().getName();
				musicKeys.add(generatePrimaryKey());
			} else {
				String tbl;
				String where = "";
				if (stmt instanceof Update){
					Update u = (Update) stmt;
					tbl = u.getTables().get(0).getName();
					where = u.getWhere().toString();
				} else if (stmt instanceof Delete) {
					Delete d = (Delete) stmt;
					tbl = d.getTable().getName();
					if (d.getWhere()!=null) {
						where = d.getWhere().toString();
					}
				} else {
					System.err.println("Not recognized sql type");
					tbl = "";
				}
				String dbiSelect = "SELECT * FROM " + tbl;
				if (!where.equals("")) {
					dbiSelect += "WHERE" + where;
				}
				ResultSet rs = dbi.executeSQLRead(dbiSelect);
				musicKeys.addAll(getMusicKeysWhere(tbl, Utils.parseResults(dbi.getTableInfo(tbl), rs)));
				rs.getStatement().close();
			}
		} catch (JSQLParserException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.err.print("MusicKeys:");
		for(String musicKey:musicKeys) {
			System.out.print(musicKey + ",");
		}
		*/
		return musicKeys;

	}
	
	
	/**
	 * This method gets the primary key that the music interfaces uses by default.
	 * If the front end uses a primary key, this will not match what is used in the MUSIC interface
	 * @return
	 */
	public String getMusicDefaultPrimaryKeyName() {
		return mi.getMusicDefaultPrimaryKeyName();
	}
	
	/**
	 * Asks music interface to provide the function to create a primary key
	 * e.g. uuid(), 1, "unique_aksd419fjc"
	 * @return
	 */
	public String generatePrimaryKey() {
		// TODO Auto-generated method stub
		return mi.generatePrimaryKey();
	}
	
	
	
	private class RowUpdate {
		public final String type, table;
		public final Object[] row;
		public RowUpdate(String type, String table, Object[] row) {
			this.type = type;
			this.table = table;
			this.row = new Object[row.length];
			// Make  copy of row, just to be safe...
			System.arraycopy(row, 0, this.row, 0, row.length);
		}
	}

	private List<RowUpdate> delayed_updates = new ArrayList<RowUpdate>();

	private void saveUpdate(String type, String table, Object[] row) {
		RowUpdate upd = new RowUpdate(type, table, row);
		delayed_updates.add(upd);
	}

	/**
	 * Perform a commit, as requested by the JDBC driver.  If any row updates have been delayed,
	 * they are performed now and copied into MUSIC.
	 */
	public synchronized void commit() {
		logger.info(EELFLoggerDelegate.applicationLogger, " commit ");
		// transaction was committed -- play all the updates into MUSIC
		List<RowUpdate> mylist = delayed_updates;
		delayed_updates = new ArrayList<RowUpdate>();
		logger.info(EELFLoggerDelegate.applicationLogger, " Row Update "+mylist.size());
		for (RowUpdate upd : mylist) {
			if (upd.type.equals("update")) {
				try {
					mi.updateDirtyRowAndEntityTableInMusic(upd.table, upd.row);
				}catch(Exception e) {
					logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
				}
			} else if (upd.type.equals("delete")) {
				try {
					mi.deleteFromEntityTableInMusic(upd.table, upd.row);
				}catch(Exception e) {
					logger.error(EELFLoggerDelegate.errorLogger, e.getMessage(), AppMessages.QUERYERROR, ErrorTypes.QUERYERROR, ErrorSeverity.CRITICAL);
				}
			}
		}
	}

	/**
	 * Perform a rollback, as requested by the JDBC driver.  If any row updates have been delayed,
	 * they are discarded.
	 */
	public synchronized void rollback() {
		// transaction was rolled back - discard the updates
		logger.info("Rollback");;
		delayed_updates.clear();
	}
	public String getMusicKeysFromRow(String table, Object[] dbRow) {
		return mi.getMusicKeyFromRow(table, dbRow);
	}
}
