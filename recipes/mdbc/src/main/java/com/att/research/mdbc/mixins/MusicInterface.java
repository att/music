package com.att.research.mdbc.mixins;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

/**
 * This Interface defines the methods that MDBC needs for a class to provide access to the persistence layer of MUSIC.
 *
 * @author Robert P. Eby
 */
public interface MusicInterface {	
	/**
	 * Get the name of this MusicInterface mixin object.
	 * @return the name
	 */
	public String getMixinName();
	/**
	 * Gets the name of this MusicInterface mixin's default primary key name
	 * @return default primary key name
	 */
	public String getMusicDefaultPrimaryKeyName();
	/**
	 * generates a key or placeholder for what is required for a primary key
	 * @return a primary key
	 */
	public String generatePrimaryKey();
	/**
	 * Select * from [table] where [where string]
	 * @param where
	 * @return list of primary keys
	 */
	public String getMusicKeyFromRow(String table, Object[] dbRow);
	/**
	 * Do what is needed to close down the MUSIC connection.
	 */
	public void close();
	/**
	 * This method creates a keyspace in Music/Cassandra to store the data corresponding to the SQL tables.
	 * The keyspace name comes from the initialization properties passed to the JDBC driver.
	 */
	public void createKeyspace();
	/**
	 * This method performs all necessary initialization in Music/Cassandra to store the table <i>tableName</i>.
	 * @param tableName the table to initialize MUSIC for
	 */
	public void initializeMusicForTable(String tableName);
	/**
	 * Create a <i>dirty row</i> table for the real table <i>tableName</i>.  The primary keys columns from the real table are recreated in
	 * the dirty table, along with a "REPLICA__" column that names the replica that should update it's internal state from MUSIC.
	 * @param tableName the table to create a "dirty" table for
	 */
	public void createDirtyRowTable(String tableName);
	/**
	 * Drop the dirty row table for <i>tableName</i> from MUSIC.
	 * @param tableName the table being dropped
	 */
	public void dropDirtyRowTable(String tableName);
	/**
	 * Drops the named table and its dirty row table (for all replicas) from MUSIC.  The dirty row table is dropped first.
	 * @param tableName This is the table that has been dropped
	 */
	public void clearMusicForTable(String tableName);
	/**
	 * Mark rows as "dirty" in the dirty rows table for <i>tableName</i>.  Rows are marked for all replicas but
	 * this one (this replica already has the up to date data).
	 * @param tableName the table we are marking dirty
	 * @param keys an ordered list of the values being put into the table.  The values that correspond to the tables'
	 * primary key are copied into the dirty row table.
	 */
	public void markDirtyRow(String tableName, Object[] keys);
	/**
	 * Remove the entries from the dirty row (for this replica) that correspond to a set of primary keys
	 * @param tableName the table we are removing dirty entries from
	 * @param keys the primary key values to use in the DELETE.  Note: this is *only* the primary keys, not a full table row.
	 */
	public void cleanDirtyRow(String tableName, Object[] keys);
	/**
	 * Get a list of "dirty rows" for a table.  The dirty rows returned apply only to this replica,
	 * and consist of a Map of primary key column names and values.
	 * @param tableName the table we are querying for
	 * @return a list of maps; each list item is a map of the primary key names and values for that "dirty row".
	 */
	public List<Map<String,Object>> getDirtyRows(String tableName);
	/**
	 * This method is called whenever there is a DELETE to a row on a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. MUSIC propagates
	 * it to the other replicas.
	 * @param tableName This is the table that has changed.
	 * @param oldRow This is a copy of the old row being deleted
	 */
	public void deleteFromEntityTableInMusic(String tableName, Object[] oldRow);
	/**
	 * This method is called whenever there is a SELECT on a local SQL table, wherein it first checks the local
	 * dirty bits table to see if there are any rows in Cassandra whose value needs to be copied to the local SQL DB.
	 * @param tableName This is the table on which the select is being performed
	 */
	public void readDirtyRowsAndUpdateDb(String tableName);
	/**
	 * This method is called whenever there is an INSERT or UPDATE to a local SQL table, wherein it updates the
	 * MUSIC/Cassandra tables (both dirty bits and actual data) corresponding to the SQL write. Music propagates
	 * it to the other replicas.
	 * @param tableName This is the table that has changed.
	 * @param changedRow This is information about the row that has changed
	 */
	public void updateDirtyRowAndEntityTableInMusic(String tableName, Object[] changedRow);
	
	public String getPrimaryKey(String tableName, Object[] changedRow);
}
