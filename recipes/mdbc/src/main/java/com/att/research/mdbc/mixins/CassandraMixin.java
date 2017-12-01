package com.att.research.mdbc.mixins;

import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.att.research.mdbc.MusicSqlManager;
import com.att.research.mdbc.TableInfo;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;

/**
 * This class provides the methods that MDBC needs to access Cassandra directly in order to provide persistence
 * to calls to the user's DB.  It does not do any table or row locking.
 *
 * <p>This code only supports the following limited list of H2 and Cassandra data types:</p>
 * <table summary="">
 * <tr><th>H2 Data Type</th><th>Mapped to Cassandra Data Type</th></tr>
 * <tr><td>BIGINT</td><td>BIGINT</td></tr>
 * <tr><td>BOOLEAN</td><td>BOOLEAN</td></tr>
 * <tr><td>CLOB</td><td>BLOB</td></tr>
 * <tr><td>DOUBLE</td><td>DOUBLE</td></tr>
 * <tr><td>INTEGER</td><td>INT</td></tr>
 *  <tr><td>TIMESTAMP</td><td>TIMESTAMP</td></tr>
 * <tr><td>VARBINARY</td><td>BLOB</td></tr>
 * <tr><td>VARCHAR</td><td>VARCHAR</td></tr>
 * </table>
 *
 * @author Robert P. Eby
 */
public class CassandraMixin implements MusicInterface {
	/** The property name to use to identify this replica to MusicSqlManager */
	public static final String KEY_MY_ID              = "myid";
	/** The property name to use for the comma-separated list of replica IDs. */
	public static final String KEY_REPLICAS           = "replicas";
	/** The property name to use to name the Cassandra keyspace to use. */
	public static final String KEY_MUSIC_KEYSPACE     = "music_keyspace";
	/** The property name to use to identify the IP address for Cassandra. */
	public static final String KEY_MUSIC_ADDRESS      = "music_address";
	/** The property name to use to provide the replication factor for Cassandra. */
	public static final String KEY_MUSIC_RFACTOR      = "music_rfactor";
	/** The default property value to use for the Cassandra keyspace. */
	public static final String DEFAULT_MUSIC_KEYSPACE = "mdbc";
	/** The default property value to use for the Cassandra IP address. */
	public static final String DEFAULT_MUSIC_ADDRESS  = "localhost";
	/** The default property value to use for the Cassandra replication factor. */
	public static final int    DEFAULT_MUSIC_RFACTOR  = 2;

	private static final Map<Integer, String> typemap         = new HashMap<Integer, String>();
	static {
		// We only support the following eight type mappings currently (from H2 -> Cassandra).
		// Anything else will likely cause a NullPointerException
		typemap.put(Types.BIGINT,    "BIGINT");	// aka. IDENTITY
		typemap.put(Types.BOOLEAN,   "BOOLEAN");
		typemap.put(Types.CLOB,      "BLOB");
		typemap.put(Types.DOUBLE,    "DOUBLE");
		typemap.put(Types.INTEGER,   "INT");
		typemap.put(Types.TIMESTAMP, "TIMESTAMP");
		typemap.put(Types.VARBINARY, "BLOB");
		typemap.put(Types.VARCHAR,   "VARCHAR");
	}

	protected final Logger logger;
	protected final DBInterface dbi;
	protected final String music_ns;
	protected final String myId;
	protected final String[] allReplicaIds;
	private final String musicAddress;
	private final int    music_rfactor;
	private MusicConnector mCon        = null;
	private Session musicSession       = null;
	private boolean keyspace_created   = false;
	private Map<String, PreparedStatement> ps_cache = new HashMap<String, PreparedStatement>();
	private Set<String> in_progress    = Collections.synchronizedSet(new HashSet<String>());

	public CassandraMixin() {
		this.logger         = null;
		this.dbi            = null;
		this.musicAddress   = null;
		this.music_ns       = null;
		this.music_rfactor  = 0;
		this.myId           = null;
		this.allReplicaIds  = null;
	}

	public CassandraMixin(MusicSqlManager msm, DBInterface dbi, String url, Properties info) {
		this.logger = Logger.getLogger(this.getClass());
		this.dbi = dbi;
		// Default values -- should be overridden in the Properties
		// Default to using the host_ids of the various peers as the replica IDs (this is probably preferred)
		this.musicAddress   = info.getProperty(KEY_MUSIC_ADDRESS, DEFAULT_MUSIC_ADDRESS);
		this.music_ns       = info.getProperty(KEY_MUSIC_KEYSPACE, DEFAULT_MUSIC_KEYSPACE);
		String s            = info.getProperty(KEY_MUSIC_RFACTOR);
		this.music_rfactor  = (s == null) ? DEFAULT_MUSIC_RFACTOR : Integer.parseInt(s);
		this.myId           = info.getProperty(KEY_MY_ID,    getMyHostId());
		this.allReplicaIds  = info.getProperty(KEY_REPLICAS, getAllHostIds()).split(",");
		logger.info("MusicSqlManager: myId="+myId);
		logger.info("MusicSqlManager: allReplicaIds="+info.getProperty(KEY_REPLICAS, this.myId));
		logger.info("MusicSqlManager: musicAddress="+musicAddress);
		logger.info("MusicSqlManager: music_ns="+music_ns);
	}

	private String getMyHostId() {
		ResultSet rs = executeMusicRead("SELECT HOST_ID FROM SYSTEM.LOCAL");
		Row row = rs.one();
		return (row == null) ? "UNKNOWN" : row.getUUID("HOST_ID").toString();
	}
	private String getAllHostIds() {
		ResultSet results = executeMusicRead("SELECT HOST_ID FROM SYSTEM.PEERS");
		StringBuilder sb = new StringBuilder(myId);
		for (Row row : results) {
			sb.append(",");
			sb.append(row.getUUID("HOST_ID").toString());
		}
		return sb.toString();
	}

	/**
	 * Get the name of this MusicInterface mixin object.
	 * @return the name
	 */
	@Override
	public String getMixinName() {
		return "cassandra";
	}
	/**
	 * Do what is needed to close down the MUSIC connection.
	 */
	@Override
	public void close() {
		if (musicSession != null) {
			musicSession.close();
			musicSession = null;
		}
	}

	/**
	 * This method creates a keyspace in Music/Cassandra to store the data corresponding to the SQL tables.
	 * The keyspace name comes from the initialization properties passed to the JDBC driver.
	 */
	@Override
	public void createKeyspace() {
		if (keyspace_created == false) {
			String cql = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : %d };", music_ns, music_rfactor);
			executeMusicWriteQuery(cql);
			keyspace_created = true;
		}
	}

	/**
	 * This method performs all necessary initialization in Music/Cassandra to store the table <i>tableName</i>.
	 * @param tableName the table to initialize MUSIC for
	 */
	@Override
	public void initializeMusicForTable(String tableName) {
		/**
		 * This code creates two tables for every table in SQL:
		 * (i) a table with the exact same name as the SQL table storing the SQL data.
		 * (ii) a "dirty bits" table that stores the keys in the Cassandra table that are yet to be
		 * updated in the SQL table (they were written by some other node).
		 */
		TableInfo ti = dbi.getTableInfo(tableName);
		StringBuilder fields = new StringBuilder();
		StringBuilder prikey = new StringBuilder();
		String pfx = "", pfx2 = "";
		for (int i = 0; i < ti.columns.size(); i++) {
			fields.append(pfx)
			      .append(ti.columns.get(i))
			      .append(" ")
			      .append(typemap.get(ti.coltype.get(i)));
			if (ti.iskey.get(i)) {
				// Primary key column
				prikey.append(pfx2).append(ti.columns.get(i));
				pfx2 = ", ";
			}
			pfx = ", ";
		}
		String cql = String.format("CREATE TABLE IF NOT EXISTS %s.%s (%s, PRIMARY KEY (%s));", music_ns, tableName, fields.toString(), prikey.toString());
		executeMusicWriteQuery(cql);
	}

	// **************************************************
	// Dirty Tables (in MUSIC) methods
	// **************************************************

	/**
	 * Create a <i>dirty row</i> table for the real table <i>tableName</i>.  The primary keys columns from the real table are recreated in
	 * the dirty table, along with a "REPLICA__" column that names the replica that should update it's internal state from MUSIC.
	 * @param tableName the table to create a "dirty" table for
	 */
	@Override
	public void createDirtyRowTable(String tableName) {
		// create dirtybitsTable at all replicas
//		for (String repl : allReplicaIds) {
////			String dirtyRowsTableName = "dirty_"+tableName+"_"+allReplicaIds[i];
////			String dirtyTableQuery = "CREATE TABLE IF NOT EXISTS "+music_ns+"."+ dirtyRowsTableName+" (dirtyRowKeys text PRIMARY KEY);";
//			cql = String.format("CREATE TABLE IF NOT EXISTS %s.DIRTY_%s_%s (dirtyRowKeys TEXT PRIMARY KEY);", music_ns, tableName, repl);
//			executeMusicWriteQuery(cql);
//		}
		TableInfo ti = dbi.getTableInfo(tableName);
		StringBuilder ddl = new StringBuilder("REPLICA__ TEXT");
		StringBuilder cols = new StringBuilder("REPLICA__");
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				// Only use the primary keys columns in the "Dirty" table
				ddl.append(", ")
				   .append(ti.columns.get(i))
				   .append(" ")
				   .append(typemap.get(ti.coltype.get(i)));
				cols.append(", ").append(ti.columns.get(i));
			}
		}
		ddl.append(", PRIMARY KEY(").append(cols).append(")");
		String cql = String.format("CREATE TABLE IF NOT EXISTS %s.DIRTY_%s (%s);", music_ns, tableName, ddl.toString());
		executeMusicWriteQuery(cql);
	}
	/**
	 * Drop the dirty row table for <i>tableName</i> from MUSIC.
	 * @param tableName the table being dropped
	 */
	@Override
	public void dropDirtyRowTable(String tableName) {
		String cql = String.format("DROP TABLE %s.DIRTY_%s;", music_ns, tableName);
		executeMusicWriteQuery(cql);
	}
	/**
	 * Mark rows as "dirty" in the dirty rows table for <i>tableName</i>.  Rows are marked for all replicas but
	 * this one (this replica already has the up to date data).
	 * @param tableName the table we are marking dirty
	 * @param keys an ordered list of the values being put into the table.  The values that correspond to the tables'
	 * primary key are copied into the dirty row table.
	 */
	@Override
	public void markDirtyRow(String tableName, Object[] keys) {
		TableInfo ti = dbi.getTableInfo(tableName);
		StringBuilder cols = new StringBuilder("REPLICA__");
		StringBuilder vals = new StringBuilder("?");
		List<Object> vallist = new ArrayList<Object>();
		vallist.add(""); // placeholder for replica
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				cols.append(", ").append(ti.columns.get(i));
				vals.append(", ").append("?");
				vallist.add(keys[i]);
			}
		}
		String cql = String.format("INSERT INTO %s.DIRTY_%s (%s) VALUES (%s);", music_ns, tableName, cols.toString(), vals.toString());
		Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		for (String repl : allReplicaIds) {
			if (!repl.equals(myId)) {
				logger.debug("Executing MUSIC write:"+ cql);
				vallist.set(0, repl);
				BoundStatement bound = ps.bind(vallist.toArray());
				bound.setReadTimeoutMillis(60000);
				synchronized (sess) {
					sess.execute(bound);
				}
			}
		}
	}
	/**
	 * Remove the entries from the dirty row (for this replica) that correspond to a set of primary keys
	 * @param tableName the table we are removing dirty entries from
	 * @param keys the primary key values to use in the DELETE.  Note: this is *only* the primary keys, not a full table row.
	 */
	@Override
	public void cleanDirtyRow(String tableName, Object[] keys) {
		TableInfo ti = dbi.getTableInfo(tableName);
		StringBuilder cols = new StringBuilder("REPLICA__=?");
		List<Object> vallist = new ArrayList<Object>();
		vallist.add(myId);
		int n = 0;
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				cols.append(" AND ").append(ti.columns.get(i)).append("=?");
				vallist.add(keys[n++]);
			}
		}
		String cql = String.format("DELETE FROM %s.DIRTY_%s WHERE %s;", music_ns, tableName, cols.toString());
		logger.debug("Executing MUSIC write:"+ cql);
		Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(vallist.toArray());
		bound.setReadTimeoutMillis(60000);
		synchronized (sess) {
			sess.execute(bound);
		}
	}
	/**
	 * Get a list of "dirty rows" for a table.  The dirty rows returned apply only to this replica,
	 * and consist of a Map of primary key column names and values.
	 * @param tableName the table we are querying for
	 * @return a list of maps; each list item is a map of the primary key names and values for that "dirty row".
	 */
	@Override
	public List<Map<String,Object>> getDirtyRows(String tableName) {
		String cql = String.format("SELECT * FROM %s.DIRTY_%s WHERE REPLICA__=?;", music_ns, tableName);
		ResultSet results = null;
		logger.debug("Executing MUSIC write:"+ cql);
		Session sess = getMusicSession();
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(new Object[] { myId });
		bound.setReadTimeoutMillis(60000);
		synchronized (sess) {
			results = sess.execute(bound);
		}
		ColumnDefinitions cdef = results.getColumnDefinitions();
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for (Row row : results) {
			Map<String,Object> objs = new HashMap<String,Object>();
			for (int i = 0; i < cdef.size(); i++) {
				String colname = cdef.getName(i).toUpperCase();
				String coltype = cdef.getType(i).getName().toString().toUpperCase();
				if (!colname.equals("REPLICA__")) {
					switch (coltype) {
					case "BIGINT":
						objs.put(colname, row.getLong(colname));
						break;
					case "BOOLEAN":
						objs.put(colname, row.getBool(colname));
						break;
					case "BLOB":
						logger.error("WE DO NOT SUPPORT BLOBS AS PRIMARY KEYS!! COLUMN NAME="+colname);
						// throw an exception here???
						break;
					case "DOUBLE":
						objs.put(colname, row.getDouble(colname));
						break;
					case "INT":
						objs.put(colname, row.getInt(colname));
						break;
					case "TIMESTAMP":
						objs.put(colname, row.getTimestamp(colname));
						break;
					case "VARCHAR":
					default:
						objs.put(colname, row.getString(colname));
						break;
					}
				}
			}
			list.add(objs);
		}
		return list;
	}

	/**
	 * Drops the named table and its dirty row table (for all replicas) from MUSIC.  The dirty row table is dropped first.
	 * @param tableName This is the table that has been dropped
	 */
	@Override
	public void clearMusicForTable(String tableName) {
		dropDirtyRowTable(tableName);
		String cql = String.format("DROP TABLE %s.%s;", music_ns, tableName);
		executeMusicWriteQuery(cql);
	}
	/**
	 * This function is called whenever there is a DELETE to a row on a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. MUSIC propagates
	 * it to the other replicas.
	 *
	 * @param tableName This is the table that has changed.
	 * @param oldRow This is a copy of the old row being deleted
	 */
	@Override
	public void deleteFromEntityTableInMusic(String tableName, Object[] oldRow) {
		TableInfo ti = dbi.getTableInfo(tableName);
		assert(ti.columns.size() == oldRow.length);

		StringBuilder where = new StringBuilder();
		List<Object> vallist = new ArrayList<Object>();
		String pfx = "";
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				where.append(pfx)
				     .append(ti.columns.get(i))
				     .append("=?");
				vallist.add(oldRow[i]);
				pfx = " AND ";
			}
		}

		String cql = String.format("DELETE FROM %s.%s WHERE %s;", music_ns, tableName, where.toString());
		logger.debug("Executing MUSIC write:"+ cql);
		PreparedStatement ps = getPreparedStatementFromCache(cql);
		BoundStatement bound = ps.bind(vallist.toArray());
		bound.setReadTimeoutMillis(60000);
		Session sess = getMusicSession();
		synchronized (sess) {
			sess.execute(bound);
		}

		// Mark the dirty rows in music for all the replicas but us
		markDirtyRow(tableName, oldRow);
	}

	public Set<String> getMusicTableSet(String ns) {
		Set<String> set = new TreeSet<String>();
		String cql = String.format("SELECT TABLE_NAME FROM SYSTEM_SCHEMA.TABLES WHERE KEYSPACE_NAME = '%s'", ns);
		ResultSet rs = executeMusicRead(cql);
		for (Row row : rs) {
			set.add(row.getString("TABLE_NAME").toUpperCase());
		}
		return set;
	}
	/**
	 * This method is called whenever there is a SELECT on a local SQL table, wherein it first checks the local
	 * dirty bits table to see if there are any keys in Cassandra whose value has not yet been sent to SQL
	 * @param tableName This is the table on which the select is being performed
	 */
	@Override
	public void readDirtyRowsAndUpdateDb(String tableName) {
		// Read dirty rows of this table from Music
		List<Map<String,Object>> objlist = getDirtyRows(tableName);
		String pre_cql = String.format("SELECT * FROM %s.%s WHERE ", music_ns, tableName);
		List<Object> vallist = new ArrayList<Object>();
		StringBuilder sb = new StringBuilder();
		for (Map<String,Object> map : objlist) {
			sb.setLength(0);
			vallist.clear();
			String pfx = "";
			for (String key : map.keySet()) {
				sb.append(pfx).append(key).append("=?");
				vallist.add(map.get(key));
				pfx = " AND ";
			}
			Session sess = getMusicSession();
			String cql = pre_cql + sb.toString();
			PreparedStatement ps = getPreparedStatementFromCache(cql);
			BoundStatement bound = ps.bind(vallist.toArray());
			bound.setReadTimeoutMillis(60000);
			ResultSet dirtyRows = null;
			synchronized (sess) {
				dirtyRows = sess.execute(bound);
			}
			List<Row> rows = dirtyRows.all();
			if (rows.isEmpty()) {
				// No rows, the row must have been deleted
				deleteRowFromSqlDb(tableName, map);
			} else {
				for (Row row : rows) {
					writeMusicRowToSQLDb(tableName, row);
				}
			}
		}
	}

	private void deleteRowFromSqlDb(String tableName, Map<String, Object> map) {
		dbi.deleteRowFromSqlDb(tableName, map);
		TableInfo ti = dbi.getTableInfo(tableName);
		List<Object> vallist = new ArrayList<Object>();
		for (int i = 0; i < ti.columns.size(); i++) {
			if (ti.iskey.get(i)) {
				String col = ti.columns.get(i);
				Object val = map.get(col);
				vallist.add(val);
			}
		}
		cleanDirtyRow(tableName, vallist.toArray());
	}
	/**
	 * This functions copies the contents of a row in Music into the corresponding row in the SQL table
	 * @param tableName This is the name of the table in both Music and swl
	 * @param musicRow This is the row in Music that is being copied into SQL
	 */
	private void writeMusicRowToSQLDb(String tableName, Row musicRow) {
		// First construct the map of columns and their values
		TableInfo ti = dbi.getTableInfo(tableName);
		Map<String, Object> map = new HashMap<String, Object>();
		List<Object> vallist = new ArrayList<Object>();
		String rowid = tableName;
		for (String col : ti.columns) {
			Object val = getValue(musicRow, col);
			map.put(col, val);
			if (ti.iskey(col)) {
				vallist.add(val);
				rowid += "_" + val.toString();
			}
		}

		logger.debug("Blocking rowid: "+rowid);
		in_progress.add(rowid);			// Block propagation of the following INSERT/UPDATE

		dbi.insertRowIntoSqlDb(tableName, map);

		logger.debug("Unblocking rowid: "+rowid);
		in_progress.remove(rowid);		// Unblock propagation

//		try {
//			String sql = String.format("INSERT INTO %s (%s) VALUES (%s);", tableName, fields.toString(), values.toString());
//			executeSQLWrite(sql);
//		} catch (SQLException e) {
//			logger.debug("Insert failed because row exists, do an update");
//			// TODO - rewrite this UPDATE command should not update key fields
//			String sql = String.format("UPDATE %s SET (%s) = (%s) WHERE %s", tableName, fields.toString(), values.toString(), where.toString());
//			try {
//				executeSQLWrite(sql);
//			} catch (SQLException e1) {
//				e1.printStackTrace();
//			}
//		}

		cleanDirtyRow(tableName, vallist.toArray());

//		String selectQuery = "select "+ primaryKeyName+" FROM "+tableName+" WHERE "+primaryKeyName+"="+primaryKeyValue+";";
//		java.sql.ResultSet rs = executeSQLRead(selectQuery);
//		String dbWriteQuery=null;
//		try {
//			if(rs.next()){//this entry is there, do an update
//				dbWriteQuery = "UPDATE "+tableName+" SET "+columnNameString+" = "+ valueString +"WHERE "+primaryKeyName+"="+primaryKeyValue+";";
//			}else
//				dbWriteQuery = "INSERT INTO "+tableName+" VALUES"+valueString+";";
//			executeSQLWrite(dbWriteQuery);
//		} catch (SQLException e) {
//			// ZZTODO Auto-generated catch block
//			e.printStackTrace();
//		}

		//clean the music dirty bits table
//		String dirtyRowIdsTableName = music_ns+".DIRTY_"+tableName+"_"+myId;
//		String deleteQuery = "DELETE FROM "+dirtyRowIdsTableName+" WHERE dirtyRowKeys=$$"+primaryKeyValue+"$$;";
//		executeMusicWriteQuery(deleteQuery);
	}
	private Object getValue(Row musicRow, String colname) {
		ColumnDefinitions cdef = musicRow.getColumnDefinitions();
		String type = cdef.getType(colname).getName().toString().toUpperCase();
		switch (type) {
		case "BIGINT":
			return musicRow.getLong(colname);
		case "BOOLEAN":
			return musicRow.getBool(colname);
		case "BLOB":
			return musicRow.getBytes(colname);
		case "DOUBLE":
			return musicRow.getDouble(colname);
		case "INT":
			return musicRow.getInt(colname);
		case "TIMESTAMP":
			return musicRow.getTimestamp(colname);
		default:
			logger.error("UNEXPECTED COLUMN TYPE: columname="+colname+", columntype="+type);
			// fall thru
		case "VARCHAR":
			return musicRow.getString(colname);
		}
	}

	/**
	 * This method is called whenever there is an INSERT or UPDATE to a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. Music propagates
	 * it to the other replicas.
	 *
	 * @param tableName This is the table that has changed.
	 * @param changedRow This is information about the row that has changed
	 */
	@Override
	public void updateDirtyRowAndEntityTableInMusic(String tableName, Object[] changedRow) {
		// Build the CQL command
		TableInfo ti = dbi.getTableInfo(tableName);
		StringBuilder fields = new StringBuilder();
		StringBuilder values = new StringBuilder();
		String rowid = tableName;
		Object[] newrow = new Object[changedRow.length];
		String pfx = "";
		for (int i = 0; i < changedRow.length; i++) {
			fields.append(pfx).append(ti.columns.get(i));
			values.append(pfx).append("?");
			pfx = ", ";
			if (changedRow[i] instanceof byte[]) {
				// Cassandra doesn't seem to have a Codec to translate a byte[] to a ByteBuffer
				newrow[i] = ByteBuffer.wrap((byte[]) changedRow[i]);
			} else if (changedRow[i] instanceof Reader) {
				// Cassandra doesn't seem to have a Codec to translate a Reader to a ByteBuffer either...
				newrow[i] = ByteBuffer.wrap(readBytesFromReader((Reader) changedRow[i]));
			} else {
				newrow[i] = changedRow[i];
			}
			if (ti.iskey.get(i)) {
				rowid += "_" + newrow[i].toString();
			}
		}

		if (in_progress.contains(rowid)) {
			// This call to updateDirtyRowAndEntityTableInMusic() was called as a result of a Cassandra -> H2 update; ignore
			logger.debug("updateDirtyRowAndEntityTableInMusic: bypassing MUSIC update on "+rowid);
		} else {
			// Update local MUSIC node. Note: in Cassandra you can insert again on an existing key..it becomes an update
			String cql = String.format("INSERT INTO %s.%s (%s) VALUES (%s);", music_ns, tableName, fields.toString(), values.toString());
			logger.debug("Executing MUSIC write:"+ cql);
			PreparedStatement ps = getPreparedStatementFromCache(cql);
			BoundStatement bound = ps.bind(newrow);
			bound.setReadTimeoutMillis(60000);
			Session sess = getMusicSession();
			synchronized (sess) {
				sess.execute(bound);
			}

			// Mark the dirty rows in music for all the replicas but us
			markDirtyRow(tableName, changedRow);
		}
	}
	private byte[] readBytesFromReader(Reader rdr) {
		StringBuilder sb = new StringBuilder();
		try {
			int ch;
			while ((ch = rdr.read()) >= 0) {
				sb.append((char)ch);
			}
		} catch (IOException e) {
			logger.warn("readBytesFromReader: "+e);
		}
		return sb.toString().getBytes();
	}

	protected PreparedStatement getPreparedStatementFromCache(String cql) {
		// Note: have to hope that the Session never changes!
		if (!ps_cache.containsKey(cql)) {
			Session sess = getMusicSession();
			PreparedStatement ps = sess.prepare(cql);
			ps_cache.put(cql, ps);
		}
		return ps_cache.get(cql);
	}

	/**
	 * This method gets a connection to Music
	 * @return the Cassandra Session to use
	 */
	protected Session getMusicSession() {
		// create cassandra session
		if (musicSession == null) {
			logger.debug("New Music session created");
			mCon = new MusicConnector(musicAddress);
			musicSession = mCon.getSession();
		}
		return musicSession;
	}

	/**
	 * This method executes a write query in Music
	 * @param cql the CQL to be sent to Cassandra
	 */
	protected void executeMusicWriteQuery(String cql) {
		logger.debug("Executing MUSIC write:"+ cql);
		Session sess = getMusicSession();
		SimpleStatement s = new SimpleStatement(cql);
		s.setReadTimeoutMillis(60000);
		synchronized (sess) {
			sess.execute(s);
		}
	}

	/**
	 * This method executes a read query in Music
	 * @param cql the CQL to be sent to Cassandra
	 * @return a ResultSet containing the rows returned from the query
	 */
	protected ResultSet executeMusicRead(String cql) {
		logger.debug("Executing MUSIC read:"+ cql);
		Session sess = getMusicSession();
		synchronized (sess) {
			return sess.execute(cql);
		}
	}
}
