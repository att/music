/*
 * 
This licence applies to all files in this repository unless otherwise specifically
stated inside of the file. 

 ---------------------------------------------------------------------------
   Copyright (c) 2016 AT&T Intellectual Property

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at:

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ---------------------------------------------------------------------------

 */
package com.att.research.music.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.att.research.music.datastore.MusicDataStore;
import com.att.research.music.datastore.jsonobjects.JsonInsert;
import com.att.research.music.datastore.jsonobjects.JsonKeySpace;
import com.att.research.music.datastore.jsonobjects.JsonTable;
import com.att.research.music.lockingservice.MusicLockingService;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * A MUSIC client that talks directly to Cassandra/ZooKeeper.  This was taken, and
 * slightly modified, from the REST version of the code.
 *
 * @author Robert Eby
 */
public class MusicDirectClient {
    private static final Logger LOG = LoggerFactory.getLogger(MusicDirectClient.class);

    private final String[] music_hosts;	// array of hosts in the music cluster
	private Cluster cluster;			// MUSIC Cassandra cluster
	private Session session;			// MUSIC Cassandra session
	private MusicLockingService mls;	// ZooKeeper
	private final Set<String> lockNames;// set of active lock names

	/**
	 * Create a MUSIC client that talks to MUSIC on localhost.
	 */
	public MusicDirectClient() {
		this("127.0.0.1");
	}
	/**
	 * Create a MUSIC client that talks to MUSIC on a remote host.  The string <i>hosts</i>
	 * is a comma-separated list of IP addresses for remote instances of Cassandra/ZooKeeper.
	 * @param hosts the list of hostnames
	 */
	public MusicDirectClient(String hosts) {
		music_hosts = hosts.split(",");
		if (cluster == null) {
			LOG.debug("Initializing MUSIC Client with endpoints "+hosts);
			cluster = Cluster.builder()
				.addContactPoints(music_hosts)
				.build();
		}
		session = cluster.connect();
		mls = null;
		lockNames = new HashSet<String>();
	}
	/**
	 * Close the connection to MUSIC.
	 */
	public void close() {
		if (session != null) {
			session.close();
			session = null;
		}
		if (cluster != null) {
			cluster.close();
			cluster = null;
		}
	}
	/**
	 * Be sure to close the connection to MUSIC when this object is GC-ed.
	 */
	@Override
	protected void finalize() {
		close();
	}
	/**
	 * Return a String representation of the music hosts used by this object.
	 * @return the string
	 */
	@Override
	public String toString() {
		List<String> t = Arrays.asList(music_hosts);
		return "MUSIC hosts=" + t.toString();
	}
	/**
	 * Create a lock.
	 * @see com.att.research.music.lockingservice.MusicLockingService#createLock(String)
	 * @param lockName the lock name
	 * @return FILL IN
	 */
	public String createLock(String lockName) {
		String ln = "/"+lockName;
		synchronized (lockNames) {
			lockNames.add(ln);
		}
		return getLockingService().createLockId(ln);
	}
	/**
	 * Acquire a lock.
	 * @see com.att.research.music.lockingservice.MusicLockingService#lock(String)
	 * @param lockName the lock name
	 * @return FILL IN
	 */
	public boolean acquireLock(String lockName) {
		return getLockingService().isMyTurn(lockName);
	}
	/**
	 * Get the lock holder.
	 * @see com.att.research.music.lockingservice.MusicLockingService#currentLockHolder(String)
	 * @param lockName the lock name
	 * @return FILL IN
	 */
	public String getLockHolder(String lockName) {
		return getLockingService().whoseTurnIsIt("/"+lockName);
	}
	/**
	 * Unlock a lock.
	 * @see com.att.research.music.lockingservice.MusicLockingService#unlock(String)
	 * @param lockName the lock name
	 */
	public void unlockLock(String lockName) {
		getLockingService().unlockAndDeleteId(lockName);
	}
	/**
	 * Delete a lock.
	 * @see com.att.research.music.lockingservice.MusicLockingService#deleteLock(String)
	 * @param lockName the lock name
	 */
	public void deleteLock(String lockName) {
		String ln = "/"+lockName;
		synchronized (lockNames) {
			lockNames.remove(ln);
		}
		getLockingService().deleteLock(ln);
	}
	/**
	 * Delete all locks.
	 * @see com.att.research.music.lockingservice.MusicLockingService#deleteLock(String)
	 * @return true
	 */
	public boolean deleteAllLocks() {
		synchronized (lockNames) {
			for (String lockName : lockNames) {
				deleteLock(lockName);
			}
			lockNames.clear();
		}
		return true;
	}

	/**
	 * Create a keyspace using the default replication configuration.
	 * @param keyspaceName the name of the keyspace
	 * @return always true currently
	 * @throws Exception Cassandra exceptions are passed through
	 */
	public boolean createKeyspace(String keyspaceName) throws Exception {
		Map<String, Object> repl = new HashMap<String, Object>();
		repl.put("class", "SimpleStrategy");
		repl.put("replication_factor", 1);
		Map<String, String> consistencyInfo = Collections.singletonMap("type", "eventual");
		JsonKeySpace jsonKp = new JsonKeySpace();
		jsonKp.setConsistencyInfo(consistencyInfo);
		jsonKp.setDurabilityOfWrites("true");
		jsonKp.setReplicationInfo(repl);
		return createKeyspace(keyspaceName, jsonKp);
	}
	public boolean createKeyspace(String keyspaceName, JsonKeySpace kspObject) throws Exception {
		String consistency = extractConsistencyInfo(keyspaceName, kspObject.getConsistencyInfo());
		Map<String,Object> replicationInfo = kspObject.getReplicationInfo();
		String durability = "";
		if (kspObject.getDurabilityOfWrites() != null)
			durability = " AND durable_writes = " + kspObject.getDurabilityOfWrites();
		String query = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { %s } %s;",
			keyspaceName, jsonMaptoSqlString(replicationInfo,","), durability);
		LOG.debug(query);
		executeCreateQuery(query, consistency);
		return true;
	}
	public boolean dropKeyspace(String keyspaceName, JsonKeySpace kspObject) throws Exception {
		String consistency = extractConsistencyInfo(keyspaceName, kspObject.getConsistencyInfo());
		String query = String.format("DROP KEYSPACE %s;", keyspaceName);
		LOG.debug(query);
		executeCreateQuery(query,consistency);
		return false;
	}

	public boolean createTable(String tablename, Map<String, String> cols) throws Exception {
		JsonTable tableObj = new JsonTable();
		Map<String,String> map = new HashMap<String,String>();	// This should be in the consutructor!
		map.put("type", "eventual");
		tableObj.setConsistencyInfo(map);
		return createTable(tablename, cols, tableObj);
	}
	public boolean createTable(String tablename, Map<String, String> cols, JsonTable tableObj) throws Exception {
		// Note: https://docs.datastax.com/en/cql/3.0/cql/cql_reference/create_table_r.html

		//first read the information about the table fields
		StringBuilder fields = new StringBuilder();
		String prefix = "";
		for (String key : cols.keySet()) {
			fields.append(prefix).append(key).append(" ").append(cols.get(key));
			prefix = ", ";
		}

		//information about the name-value style properties
//		Map<String,Object> propertiesMap = tableObj.getProperties();
//		String propertiesString="";
//		if(propertiesMap != null){
//			counter =0;
//			for (Map.Entry<String, Object> entry : propertiesMap.entrySet())
//			{
//				Object ot = entry.getValue();
//				String value = ot+"";
//				if(ot instanceof String){
//					value = "'"+value+"'";
//				}else if(ot instanceof Map){
//					Map<String,Object> otMap = (Map<String,Object>)ot;
//					value = "{"+jsonMaptoSqlString(otMap, ",")+"}";
//				}
//				propertiesString = propertiesString+entry.getKey()+"="+ value+"";
//				if(counter!=propertiesMap.size()-1)
//					propertiesString = propertiesString+" AND ";
//				counter = counter +1;
//			}
//		}

		String query = String.format("CREATE TABLE IF NOT EXISTS %s (%s);", tablename, fields.toString());
//		if (propertiesMap != null)
//			query = query + " WITH "+ propertiesString;

		LOG.debug(query);
		String consistency = extractConsistencyInfo(tablename, tableObj.getConsistencyInfo());
		executeCreateQuery(query, consistency);
		return false;
	}
	public boolean dropTable(String name) {
		//TODO
		return false;
	}
	public boolean insertRow(String name, Map<String, Object> valuesMap) throws Exception {
		Map<String, String> consistencyInfo = Collections.singletonMap("type", "eventual");
		return insertRow(name, valuesMap, consistencyInfo, new JsonInsert());
	}
	public boolean insertRow(String tablename, Map<String, Object> valuesMap, Map<String, String> consistencyInfo, JsonInsert insObj) throws Exception {
		// Note: https://docs.datastax.com/en/cql/3.0/cql/cql_reference/insert_r.html
		String[] parts = tablename.split("\\.");
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(parts[0]);
		TableMetadata tableInfo =  ks.getTable(parts[1]);

		StringBuilder fields = new StringBuilder();
		StringBuilder values = new StringBuilder();
		String prefix = "";
		for (String key : valuesMap.keySet()) {
			fields.append(prefix).append(key);
			Object valueObj  = valuesMap.get(key);
			DataType colType = tableInfo.getColumn(key).getType();
			values.append(prefix).append(convertToSqlDataType(colType, valueObj));
			prefix = ", ";
		}

		String suffix = getTTLSuffix(insObj);
		String query = String.format("INSERT INTO %s (%s) VALUES (%s)%s;", tablename, fields.toString(), values.toString(), suffix);
		LOG.debug(query);

		String consistency = extractConsistencyInfo(tablename, consistencyInfo);
		executeCreateQuery(query, consistency);
		return false;
	}
	public boolean lockRow(String name, Map<String, String> cols) {
		//TODO
		return false;
	}
	/**
	 * Select ALL rows in the table.
	 * @param tablename the name of the table
	 * @return a list of maps, one map per row
	 */
	public List<Map<String, Object>> selectRows(final String tablename) {
		return selectRows(tablename, new HashMap<String, String>());
	}
	public List<Map<String, Object>> selectRows(final String tablename, Map<String, String> cols) {
		String ns = "";
		String tbl = tablename;
		int ix = tbl.indexOf('.');
		if (ix >= 0) {
			ns = tablename.substring(0, ix);
			tbl = tablename.substring(ix+1);
		}
		Select sel = QueryBuilder.select().all().from(ns, tbl);
		Statement stmt = sel;
		if (cols.size() == 1) {
			// only handles 1 WHERE value right now
			String k = cols.keySet().iterator().next();
			Clause eqclause = QueryBuilder.eq(k, cols.get(k));
			stmt = sel.where(eqclause);
		}
		ResultSet resultset = session.execute(stmt);
		List<Map<String, Object>> results = new ArrayList<Map<String,Object>>();
		for (Row row : resultset) {
			Map<String, Object> map = new HashMap<String, Object>();
			for (Definition definition : row.getColumnDefinitions()) {
				map.put(definition.getName(), readRow(row, definition.getName(), definition.getType()));
			}
			results.add(map);
		}
		return results;
	}
	private Object readRow(final Row row, final String name, final DataType colType) {
		switch (colType.getName()) {
		case BIGINT:
			return row.getLong(name);
		case BOOLEAN:
			return row.getBool(name);
		case DOUBLE:
			return row.getDouble(name);
		case FLOAT:
			return row.getFloat(name);
		case INT:
			return row.getInt(name);
		case MAP:
			return row.getMap(name, String.class, String.class);
		case UUID:
			return row.getUUID(name);
		case TEXT:
		case VARCHAR:
			return row.getString(name);
		case VARINT:
			return row.getVarint(name);
// These are not supported right now....
// ASCII
// BLOB
// COUNTER
// CUSTOM
// DECIMAL
// INET
// LIST
// SET
// TIMESTAMP
// TIMEUUID
// TUPLE
// UDT
		default:
			return null;
		}
	}

	@Deprecated
	public List<Map<String, String>> OLDselectRows(String tablename, Map<String, String> cols) {
		String query = String.format("SELECT * FROM %s", tablename);
		if (cols.size() > 0) {
			// add WHERE clause
//			String[] parts = tablename.split("\\.");
//			KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(parts[0]);
//			TableMetadata tableInfo = ks.getTable(parts[1]);
			String whereclause = " WHERE";
			String prefix = "";
			for (String key : cols.keySet()) {
				String val = cols.get(key);
//				DataType colType = tableInfo.getColumn(key).getType();
				whereclause = String.format("%s%s %s = '%s'", whereclause, prefix, key, val);
				prefix = " AND";
			}
			query += whereclause;
		}
		LOG.debug(query);
		ResultSet resultset = session.execute(query);
		List<Map<String, String>> results = new ArrayList<Map<String,String>>();
		for (Row row : resultset) {
			ColumnDefinitions colInfo = row.getColumnDefinitions();
			Map<String, String> map = new HashMap<String, String>();
			for (Definition definition : colInfo) {
			//	map.put(definition.getName(), (String)MusicDataStore.readRow(row, definition.getName(), definition.getType()));
			}
			results.add(map);
		}
		return results;
	}
	public void updateRows(String tablename, Map<String, String> cols, Map<String, Object> vals) throws Exception {
		Map<String, String> consistencyInfo = Collections.singletonMap("type", "eventual");
		updateRows(tablename, cols, vals, consistencyInfo, new JsonInsert());
	}
	public void updateRows(String tablename, Map<String, String> cols, Map<String, Object> vals, Map<String, String> consistencyInfo, JsonInsert insObj) throws Exception {
		// https://docs.datastax.com/en/cql/3.0/cql/cql_reference/update_r.html

		//obtain the field value pairs of the update
		String[] parts = tablename.split("\\.");
		KeyspaceMetadata ks = cluster.getMetadata().getKeyspace(parts[0]);
		TableMetadata tableInfo =  ks.getTable(parts[1]);

		StringBuilder fields = new StringBuilder();
		String prefix = "";
		for (String key : vals.keySet()){
			Object valueObj = vals.get(key);
			String valueString = convertToSqlDataType(tableInfo.getColumn(key).getType(), valueObj);
			fields.append(prefix).append(key).append(" = ").append(valueString);
			prefix = ", ";
		}

		//get the row specifier
		StringBuilder rows = new StringBuilder();
		String primaryKey = "";
		prefix = "";
		for (String key : cols.keySet()) {
			String indValue = cols.get(key);
			DataType colType = tableInfo.getColumn(key).getType();
			String formattedValue = convertToSqlDataType(colType, indValue);
			primaryKey = primaryKey + indValue;
			rows.append(prefix).append(key).append(" = ").append(formattedValue);
			prefix = " AND ";
		}

		String using = getTTLSuffix(insObj);
		String query = String.format("UPDATE %s%s SET %s WHERE %s;", tablename, using, fields.toString(), rows.toString());
		LOG.debug(query);

		String consistency = extractConsistencyInfo(tablename, consistencyInfo);
		executeCreateQuery(query, consistency);
	}
	public void deleteRows(String tablename, Map<String, String> cols) {
		String ns = "";
		String tbl = tablename;
		int ix = tbl.indexOf('.');
		if (ix >= 0) {
			ns = tablename.substring(0, ix);
			tbl = tablename.substring(ix+1);
		}
		Delete stmt = QueryBuilder.delete().from(ns, tbl);
		if (cols.size() == 1) {
			// only handles 1 WHERE value right now
			String k = cols.keySet().iterator().next();
			Clause eqclause = QueryBuilder.eq(k, cols.get(k));
			session.execute(stmt.where(eqclause));
		} else {
			session.execute(stmt);
		}
	}

	private String getTTLSuffix(JsonInsert insObj) {
		String ttl = insObj.getTtl();
		String timestamp = insObj.getTimestamp();
		if (ttl != null && ttl.length() > 0) {
			if (timestamp != null && timestamp.length() > 0) {
				return " USING TTL " + ttl +" AND TIMESTAMP " + timestamp;
			} else {
				return " USING TTL " + ttl;
			}
		} else if (timestamp != null && timestamp.length() > 0) {
			return " USING TIMESTAMP "+ timestamp;
		}
		return "";
	}

	private MusicLockingService getLockingService() {
		if (mls == null) {
			mls = new MusicLockingService(music_hosts[0]);
		}
		return mls;
	}

	private String extractConsistencyInfo(String key, Map<String, String> consistencyInfo) throws Exception {
		String consistency="";
		if (consistencyInfo.get("type").equalsIgnoreCase("atomic")) {
			String lockId = consistencyInfo.get("lockId");
			String lockName = lockId.substring(lockId.indexOf("$") + 1);
			lockName = lockName.substring(0, lockName.indexOf("$"));

			//first ensure that the lock name is correct before seeing if it has access
			if (!lockName.equalsIgnoreCase(key))
				throw new Exception("THIS LOCK IS NOT FOR THE KEY: "+ key);

			String lockStatus =  getLockingService().isMyTurn(lockId)+"";
			if (lockStatus.equalsIgnoreCase("false"))
				throw new Exception("YOU DO NOT HAVE THE LOCK");
			return "atomic";
		}
		if (consistencyInfo.get("type").equalsIgnoreCase("eventual"))
			return "eventual";
		throw new Exception("Consistency type "+consistency+ " unknown!!");
	}

	//utility function to parse json map into sql like string
	private String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter) {
		String sql = "";
		String prefix = "";
		for (Map.Entry<String, Object> entry : jMap.entrySet()) {
			Object ot = entry.getValue();
			String value = ot+"";
			if (ot instanceof String) {
				value = "'"+value+"'";
			}
			sql = String.format("%s%s'%s': %s", sql, prefix, entry.getKey(), value);
			prefix = lineDelimiter;
		}
		return sql;
	}
	private String convertToSqlDataType(DataType type, Object valueObj) {
		switch (type.getName()) {
		case TEXT:
			String t = valueObj.toString();
			t = t.replaceAll("'", "''");
			return "'" + t + "'";
		case MAP:
			@SuppressWarnings("unchecked")
			Map<String,Object> otMap = (Map<String,Object>) valueObj;
			return "{" + jsonMaptoSqlString(otMap, ",") + "}";
		default:
		case UUID:
			return valueObj.toString();
		}
	}
	private void executeCreateQuery(String query, String consistency) throws Exception {
		Statement statement = new SimpleStatement(query);
		if (consistency.equalsIgnoreCase("atomic"))
			statement.setConsistencyLevel(ConsistencyLevel.ALL);
		else if (consistency.equalsIgnoreCase("eventual"))
			statement.setConsistencyLevel(ConsistencyLevel.ONE);
		else
			throw new Exception("Consistency level "+consistency+ " unknown!!");
		session.execute(statement);
	}
}
