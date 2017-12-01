/**
 * <p>
 * This package provides a JDBC driver that can be used to mirror the contents of a database to and from
 * <a href="http://cassandra.apache.org/">Cassandra</a>.  The mirroring occurs as a side effect of
 * execute() statements against a JDBC connection, and triggers placed in the database to catch database modifications.
 * The initial implementation is written to mirror an <a href="http://h2database.com/">H2</a> database.
 * </p>
 * <p>
 * This JDBC driver will intercept all table creations, SELECTs, INSERTs, DELETEs, and UPDATEs made to the underlying
 * database, and make sure they are copied to Cassandra.  In addition, for every table XX that is created, another table
 * DIRTY_XX will be created to communicate the existence of <i>dirty rows</i> to other Cassandra replicas (with the
 * Cassandra2 Mixin, the table is called DIRTY____ and there is only one table).  Dirty rows
 * will be copied, as needed back into the database from Cassandra before any SELECT.
 * </p>
 * <h3>To use with JDBC</h3>
 * <ol>
 * <li>Add this jar, and all dependent jars to your CLASSPATH.</li>
 * <li>Rewrite your JDBC URLs from <code>jdbc:h2:...</code> to <code>jdbc:mdbc:...</code>.
 * <li>If you supply properties to the {@link java.sql.DriverManager#getConnection(String, java.util.Properties)} call,
 * use the following optional properties to control behavior of the proxy:
 * <table summary="">
 * <tr><th>Property Name</th><th>Property Value</th><th>Default Value</th></tr>
 * <tr><td>MDBC_DB_MIXIN</td><td>The mixin name to use to select the database mixin to use for this connection.</td></tr>
 * <tr><td>MDBC_MUSIC_MIXIN</td><td>The mixin name to use to select the MUSIC mixin to use for this connection.</td></tr>
 * <tr><td>myid</td><td>The ID of this replica in the collection of replicas sharing the same tables.</td><td>0</td></tr>
 * <tr><td>replicas</td><td>A comma-separated list of replica names for the collection of replicas sharing the same tables.</td><td>the value of <i>myid</i></td></tr>
 * <tr><td>music_keyspace</td><td>The keyspace name to use in Cassandra for all tables created by this instance of MDBC.</td><td>mdbc</td></tr>
 * <tr><td>music_address</td><td>The IP address to use to connect to Cassandra.</td><td>localhost</td></tr>
 * <tr><td>music_rfactor</td><td>The replication factor to use for the new keyspace that is created.</td><td>2</td></tr>
 * <tr><td>disabled</td><td>If set to <i>true</i> the mirroring is completely disabled; this is the equivalent of using the database driver directly.</td><td>false</td></tr>
 * </table>
 * </li>
 * <li>Load the driver using the following call:
 * <pre>
 *	Class.forName("com.att.research.mdbc.ProxyDriver");
 * </pre></li>
 * </ol>
 * <p>Because, under the current design, the MDBC driver must be running within the same JVM as the database, MDBC
 * will only explicitly support in-memory databases (URL of <code>jdbc:mdbc:mem:...</code>), or local file
 * databases (URL of <code>jdbc:mdbc:/path/to/file</code>).  Attempts to access a remote H2 server (URL
 * <code>jdbc:mdbc:tcp://host/path/to/db</code>) will probably not work, although MDBC will not stop you from trying.
 * </p>
 *
 * <h3>To Define a Tomcat DataSource Resource</h3>
 * <p>The following code snippet can be used as a guide when setting up a Tomcat DataSource Resource.
 * This snippet goes in the <i>server.xml</i> file. The items in <b>bold</b> indicate changed or new items:</p>
 * <pre>
 * &lt;Resource name="jdbc/ProcessEngine"
 *	auth="Container"
 *	type="javax.sql.DataSource"
 *	factory="org.apache.tomcat.jdbc.pool.DataSourceFactory"
 *	uniqueResourceName="process-engine"
 *	driverClassName="<b>com.att.research.mdbc.ProxyDriver</b>"
 *	url="jdbc:<b>mdbc</b>:./camunda-h2-dbs/process-engine;MVCC=TRUE;TRACE_LEVEL_FILE=0;DB_CLOSE_ON_EXIT=FALSE"
 *	<b>connectionProperties="myid=0;replicas=0,1,2;music_keyspace=camunda;music_address=localhost"</b>
 *	username="sa"
 *	password="sa"
 *	maxActive="20"
 *	minIdle="5" /&gt;
 * </pre>
 *
 * <h3>To Define a JBoss DataSource</h3>
 * <p>The following code snippet can be used as a guide when setting up a JBoss DataSource.
 * This snippet goes in the <i>service.xml</i> file. The items in <b>bold</b> indicate changed or new items:</p>
 * <pre>
 * &lt;datasources&gt;
 *   &lt;datasource jta="true" jndi-name="java:jboss/datasources/ProcessEngine" pool-name="ProcessEngine" enabled="true" use-java-context="true" use-ccm="true"&gt;
 *      &lt;connection-url&gt;jdbc:<b>mdbc</b>:/opt/jboss-eap-6.2.4/standalone/camunda-h2-dbs/process-engine;DB_CLOSE_DELAY=-1;MVCC=TRUE;DB_CLOSE_ON_EXIT=FALSE&lt;/connection-url&gt;
 *      <b>&lt;connection-property name="music_keyspace"&gt;
 *        camunda
 *      &lt;/connection-property&gt;</b>
 *      &lt;driver&gt;mdbc&lt;/driver&gt;
 *      &lt;security&gt;
 *        &lt;user-name&gt;sa&lt;/user-name&gt;
 *        &lt;password&gt;sa&lt;/password&gt;
 *      &lt;/security&gt;
 *    &lt;/datasource&gt;
 *    &lt;drivers&gt;
 *      <b>&lt;driver name="mdbc" module="com.att.research.mdbc"&gt;
 *        &lt;driver-class&gt;com.att.research.mdbc.ProxyDriver&lt;/driver-class&gt;
 *      &lt;/driver&gt;</b>
 *    &lt;/drivers&gt;
 *  &lt;/datasources&gt;
 * </pre>
 * <p>Note: This assumes that you have built and installed the <b>com.att.research.mdbc</b> module within JBoss.
 */
package com.att.research.mdbc;
