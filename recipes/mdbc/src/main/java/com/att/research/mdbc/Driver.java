package com.att.research.mdbc;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.mixins.Utils;

/**
 * ProxyDriver is a proxy to another database's JDBC driver, which also has the side
 * effect of copying table data to and from Cassandra (MUSIC) as needed to maintain replication.
 * It can be registered with JDBC by doing:
 * <pre>
 * 	Class.forName("com.att.research.mdbc.ProxyDriver");
 * </pre>
 *
 * @author Robert Eby
 */
public class Driver implements java.sql.Driver {
	/** URL prefix to use for the Proxy driver (this driver) */
	public static final String PROXY_PREFIX = "jdbc:mdbc:";
	public static final int MAJOR_VERSION = 0;
	public static final int MINOR_VERSION = 1;

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(Driver.class);

	static {
		try {
			DriverManager.registerDriver(new Driver());
			logger.info(EELFLoggerDelegate.applicationLogger, "ProxyDriver driver registered with DriverManager.");
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"SqlException while registering driver "+e);
			// ignore
		}
	}

	/**
	 * Retrieves whether the driver thinks that it can open a connection to the given URL.
	 * The proxy driver will accept any URL that starts with <i>jdbc:mdbc:</i>.
	 * @param url the URL of the database
	 * @return <code>true</code> if this driver understands the given URL; <code>false</code> otherwise
	 */
	@Override
	public boolean acceptsURL(final String url) {
		boolean b = url.startsWith(PROXY_PREFIX);
		logger.info(EELFLoggerDelegate.applicationLogger,"acceptsURL("+url+") returns "+b);
		return b;
	}

	/**
	 * Attempts to make a database connection to the underlying H2 database using a rewritten form of the given URL.
	 * The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given URL.
	 * This will be common, as when the JDBC driver manager is asked to connect to a given URL it passes the URL
	 * to each loaded driver in turn.
	 * <p>The <code>info</code> argument should contain the following parameters for both the proxy, and the underlying database.</p>
	 * <table summary="">
	 * <tr><th>Parameter</th><th>Purpose</th></tr>
	 * <tr><td>music_keyspace</td><td>The Cassandra keyspace to use to store the tables (both real and dirty row tables) for this database.
	 * If not specified, the default value is <code>mdbc</code>.</td></tr>
	 * <tr><td>music_address</td><td>The IP address or DNS name to use to connect to MUSIC.
	 * If not specified, the default value is <code>localhost</code>.</td></tr>
	 * <tr><td>music_rfactor</td><td>The replication factor to be used by MUSIC.
	 * If not specified, the default value is <code>2</code>.</td></tr>
	 * <tr><td>myid</td><td>The replica ID that this instance should use to identify itself.
	 * If not specified, the default value is <code>0</code>.</td></tr>
	 * <tr><td>replicas</td><td>A comma separated list of all the replica IDs.
	 * If not specified, the default value is the value for myid.</td></tr>
	 * <tr><td>user</td><td>A user name, passed to the underlying H2 driver.</td></tr>
	 * <tr><td>password</td><td>A password, passed to the underlying H2 driver.</td></tr>
	 * </table>
	 * @param url the URL of the database to which to connect
	 * @param info  a list of arbitrary string tag/value pairs as connection arguments. See above for required values.
	 * @return a <code>Connection</code> object that represents a connection to the URL
	 */
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		if (acceptsURL(url)) {
			String newurl = rewriteURL(url, info);
			logger.info(EELFLoggerDelegate.applicationLogger,"URL rewrite: "+url+ " to "+newurl);
			java.sql.Driver dr = null;
			try {
				dr = DriverManager.getDriver(newurl);
			} catch (SQLException sqlex) {
				logger.warn(EELFLoggerDelegate.applicationLogger,"SqlException while connect "+newurl + " "+sqlex);
				logger.warn(EELFLoggerDelegate.applicationLogger, "Registering default mdbc drivers and trying again.");
				// in JBoss modules JDBC drivers are not automatically registered
				Utils.registerDefaultDrivers();
				dr = DriverManager.getDriver(newurl);
			}
			if (dr != null) {
				Connection conn = new MdbcConnection(url, dr.connect(newurl, info), info);
				logger.info(EELFLoggerDelegate.applicationLogger,"Connection established: "+url);
				return conn;
			}
			logger.error(EELFLoggerDelegate.errorLogger, "MDBC could not find proper driver for internal url: " + newurl);
		}
		return null;
	}
	

	/**
	 * Gets the driver's major version number.
	 * @return this driver's major version number
	 */
	@Override
	public int getMajorVersion() {
		return MAJOR_VERSION;
	}
	/**
	 * Gets the driver's minor version number.
	 * @return this driver's minor version number
	 */
	@Override
	public int getMinorVersion() {
		return MINOR_VERSION;
	}
	/**
	 * Not implemented; will always throw the exception.
	 * @throws SQLFeatureNotSupportedException - if the driver does not use java.util.logging.
	 */
	@Override
	public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
		logger.info(EELFLoggerDelegate.applicationLogger,"getPropertyInfo "+url);
		String newurl = rewriteURL(url, info);
		java.sql.Driver dr = DriverManager.getDriver(newurl);
		if (dr != null) {
			DriverPropertyInfo[] dpi = dr.getPropertyInfo(newurl, info);
			List<DriverPropertyInfo> list = Arrays.asList(dpi);
			list.add(new DriverPropertyInfo(MusicSqlManager.KEY_DB_MIXIN_NAME,    info.getProperty(MusicSqlManager.KEY_DB_MIXIN_NAME,    MusicSqlManager.DB_MIXIN_DEFAULT)));
			list.add(new DriverPropertyInfo(MusicSqlManager.KEY_MUSIC_MIXIN_NAME, info.getProperty(MusicSqlManager.KEY_MUSIC_MIXIN_NAME, MusicSqlManager.MUSIC_MIXIN_DEFAULT)));
			return list.toArray(new DriverPropertyInfo[0]);
		}
		return new DriverPropertyInfo[0];
	}
	/**
	 * Reports whether this driver is a genuine JDBC CompliantTM driver. This function always returns true, although
	 * I have no idea if it is really compliant.
	 * @return true
	 */
	@Override
	public boolean jdbcCompliant() {
		return true;
	}

	private String rewriteURL(String u, Properties info) {
		logger.info(EELFLoggerDelegate.applicationLogger,"rewriteURL "+u);
		String db = info.getProperty(MusicSqlManager.KEY_DB_MIXIN_NAME, MusicSqlManager.DB_MIXIN_DEFAULT);
		if (db.equals("h2server"))
			db = "h2";	// uggh! -- special case
		return String.format("jdbc:%s:%s", db, u.substring(PROXY_PREFIX.length()));
	}
}
