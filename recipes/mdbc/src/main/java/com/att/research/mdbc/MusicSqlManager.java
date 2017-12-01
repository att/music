package com.att.research.mdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.att.research.mdbc.mixins.DBInterface;
import com.att.research.mdbc.mixins.MixinFactory;
import com.att.research.mdbc.mixins.MusicInterface;

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
	public static final String DB_MIXIN_DEFAULT     = "h2";
	/** The name of the default mixin to use for the MusicInterface. */
	public static final String MUSIC_MIXIN_DEFAULT  = "cassandra2";

	private static final Map<String, MusicSqlManager> msm_map = new Hashtable<String, MusicSqlManager>(); // needs to be synchronized

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
	 */
	public static MusicSqlManager getMusicSqlManager(String url, Connection c, Properties info) {
		String s = info.getProperty(KEY_DISABLED, "false");
		if (s.equalsIgnoreCase("true")) {
			return null;
		}

		// To support transactions we need one MusicSqlManager per Connection
		return new MusicSqlManager(url, c, info);

//		synchronized (msm_map) {
//			MusicSqlManager mgr = msm_map.get(url);
//			if (mgr == null) {
//				mgr = new MusicSqlManager(url, c, info);
//				msm_map.put(url, mgr);
//			}
//			return mgr;
//		}
	}

	private final Logger logger;
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
	 */
	private MusicSqlManager(String url, Connection conn, Properties info) {
		String mixin1  = info.getProperty(KEY_DB_MIXIN_NAME, "h2");
		String mixin2  = info.getProperty(KEY_MUSIC_MIXIN_NAME, "cassandra2");
		this.logger    = Logger.getLogger(this.getClass());
		this.dbi       = MixinFactory.createDBInterface(mixin1, this, url, conn, info);
		this.mi        = MixinFactory.createMusicInterface(mixin2, this, dbi, url, info);
		this.table_set = Collections.synchronizedSet(new HashSet<String>());
		this.autocommit = true;
		this.mi.createKeyspace();
	}

	public void setAutoCommit(boolean b) {
		if (b != autocommit) {
			autocommit = b;
			logger.info("autocommit changed to "+b);
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
		// Drop triggers from the database and from msm_map for this MusicSqlManager
		for (String tableName : table_set) {
			dbi.dropSQLTriggers(tableName);
		}
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
	 */
	public void postStatementHook(final String sql) {
		dbi.postStatementHook(sql);
	}
	/**
	 * Synchronize the list of tables in H2 with the list in MUSIC. This function should be called when the
	 * proxy first starts, and whenever there is the possibility that tables were created or dropped.  It is synchronized
	 * in order to prevent multiple threads from running this code in parallel.
	 */
	public synchronized void synchronizeTables() {
			Set<String> set1 = dbi.getSQLTableSet();	// set of tables in the database
			for (String tableName : set1) {
				// This map will be filled in if this table was previously discovered
				if (!table_set.contains(tableName)) {
					logger.debug("New table discovered: "+tableName);
					try {
						mi.initializeMusicForTable(tableName);
						mi.createDirtyRowTable(tableName);
						dbi.createSQLTriggers(tableName);
						table_set.add(tableName);
					} catch (Exception e) {
						logger.warn("synchronizeTables: "+e);
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
	 */
	public void synchronizeTableData() {
		// TODO - copy MUSIC -> H2
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
		logger.info("Commit");;
		// transaction was committed -- play all the updates into MUSIC
		List<RowUpdate> mylist = delayed_updates;
		delayed_updates = new ArrayList<RowUpdate>();
		for (RowUpdate upd : mylist) {
			if (upd.type.equals("update")) {
				mi.updateDirtyRowAndEntityTableInMusic(upd.table, upd.row);
			} else if (upd.type.equals("delete")) {
				mi.deleteFromEntityTableInMusic(upd.table, upd.row);
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
}
