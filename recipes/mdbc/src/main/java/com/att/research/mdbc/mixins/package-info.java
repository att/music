/**
 * <p>
 * This package provides the "mixins" to use when constructing a MusicSqlManager.  The mixins define how MusicSqlManager
 * will interface both to the database being mirrored (via the {@link com.att.research.mdbc.mixins.DBInterface} interface),
 * and how it will interface to the persistence layer provided by MUSIC (via the {@link com.att.research.mdbc.mixins.MusicInterface}
 * interface).
 * </p>
 * <p>
 * The choice of which mixins to use is determined by the MusicSqlManager constructor.
 * It will decide based upon the URL and connection properties with which it is presented (from the
 * {@link java.sql.DriverManager#getConnection(String, java.util.Properties)} call).
 * </p>
 * <p>
 * The list of mixins that may be selected from is stored in the properties files <code>mdbc.properties</code>
 * under the name MIXINS.  This implementation provides the following mixins:
 * </p>
 * <table summary="">
 * <tr><th>Name</th><th>Class</th><th>Description</th></tr>
 * <tr><td>cassandra</td><td>c.a.r.m.m.CassandraMixin</td><td>A <a href="http://cassandra.apache.org/">Cassandra</a> based
 * persistence layer (without any of the table locking that MUSIC normally provides).</td></tr>
 * <tr><td>cassandra2</td><td>c.a.r.m.m.Cassandra2Mixin</td><td>Similar to the <i>cassandra</i> mixin, but stores all
 * dirty row information in one table, rather than one table per real table.</td></tr>
 * <tr><td>h2</td><td>c.a.r.m.m.H2Mixin</td><td>This mixin provides access to either an in-memory, or a local
 * (file-based) version of the H2 database.</td></tr>
 * <tr><td>h2server</td><td>c.a.r.m.m.H2ServerMixin</td><td>This mixin provides access to a copy of the H2 database
 * running as a server.  Because the server needs special Java classes in order to handle certain TRIGGER actions, the
 * server must be et up in a special way (see below).</td></tr>
 * <tr><td>mysql</td><td>c.a.r.m.m.MySQLMixin</td><td>This mixin provides access to MySQL running on a remote server.</td></tr>
 * </table>
 * <h2>Starting the H2 Server</h2>
 * <p>
 * The H2 Server, when used with MDBC, must contain the MDBC Trigger class, and supporting libraries.
 * This can be done as follows:
 * </p>
 * <pre>
 *	CLASSPATH=$PWD/target/mdbc-h2server-0.0.1-SNAPSHOT.jar
 *	CLASSPATH=$CLASSPATH:$HOME/.m2/repository/com/h2database/h2/1.3.168/h2-1.3.168.jar
 *	CLASSPATH=$CLASSPATH:$HOME/.m2/repository/log4j/log4j/1.2.17/log4j-1.2.17.jar
 *	CLASSPATH=$CLASSPATH:$HOME/.m2/repository/org/json/json/20160810/json-20160810.jar
 *	export CLASSPATH
 *	java org.h2.tools.Server
 * </pre>
 * <p>
 * The <code>mdbc-h2server-0.0.1-SNAPSHOT.jar</code> file is built with Maven using the <code>pom-h2server.xml</code> pom file. 
 * </p>
 */
package com.att.research.mdbc.mixins;
