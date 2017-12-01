package com.att.research.mdbc;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * This is a utility class needed to allow external shell scripts to directly interface to a MDBC
 * database.  The external scripts cannot (should not) put data directly into Cassandra, because
 * then the dirty tables may not be set up correctly, thus, this program.
 *
 * @author Robert Eby
 */
public class SQL_Runner {
	public static void main(String[] args) throws SQLException, ClassNotFoundException, IOException {
		SQL_Runner s = new SQL_Runner(args);
		s.run();
	}

	private String url      = "jdbc:mdbc:/tmp/test";
	private String address  = "127.0.0.1";
	private String keyspace = "TEST";
	private String id       = null;
	private String replicas = null;
	private List<String> files = new ArrayList<String>();

	/**
	 * Construct an SQL_Runner instance.  After construction, the <code>run()</code> method should be
	 * called in order to run the scripts.  Arguments are as follows:
	 * <table summary="">
	 * <tr><td>-U url</td><td>the URL to use for the underlying connection</td></tr>
	 * <tr><td>-A address</td><td>the IP address to use for Cassandra</td></tr>
	 * <tr><td>-K keyspace</td><td>the keyspace name to use for Cassandra</td></tr>
	 * <tr><td>-I id</td><td>the ID to use for 'my' replica</td></tr>
	 * <tr><td>-R replicas</td><td>the number of replicas to use</td></tr>
	 * </table>
	 * @param args see table above for the arguments
	 */
	public SQL_Runner(String[] args) {
		for (int i = 0; i < args.length; i++) {
			switch (args[i]) {
			case "-U":	url = args[++i];
						break;
			case "-A":	address = args[++i];
						break;
			case "-K":	keyspace = args[++i];
						break;
			case "-I":	id = args[++i];
						break;
			case "-R":	replicas = args[++i];
						break;
			default:	files.add(args[i]);
						break;
			}
		}
		if (url == null || keyspace == null || files.size() == 0) {
			System.err.println("usage: SQL_Runner [ -U URL ] [ -K keyspace ] [ -A address ] [ -I id ] [ -R repls ] file...");
			System.exit(1);
		}
	}
	public void run() throws SQLException, ClassNotFoundException, IOException {
		Class.forName("com.att.research.mdbc.ProxyDriver");
		Properties prop = new Properties();
		if (id != null)
			prop.put("myid", id);
		if (replicas != null)
			prop.put("replicas", replicas);
		prop.put("music_keyspace", keyspace);
		prop.put("music_address", address);
//		prop.put("music_rfactor", "1");
		Connection conn = DriverManager.getConnection(url, prop);
        Statement stmt = conn.createStatement();
        for (String file : files) {
        	for (String sql : readSQL(file)) {
        		System.out.println("SQL: "+sql);
        		try {
        			stmt.execute(sql);
        		} catch (Exception e) {
        			System.err.println("oops! "+e);
        			e.printStackTrace();
        		}
        	}
        }
        stmt.close();
        conn.close();
		System.exit(0);
	}
	private List<String> readSQL(String file) throws IOException {
		List<String>    list = new ArrayList<String>();
		LineNumberReader lnr = new LineNumberReader(new FileReader(file));
		String line = lnr.readLine();
		String s = "";
		while (line != null) {
			if (! line.startsWith("--")) { // not a comment
				s += line;
				if (line.trim().endsWith(";")) {
					list.add(s);
					s = "";
				}
			}
			line = lnr.readLine();
		}
		if (!s.equals(""))
			list.add(s);
		lnr.close();
		return list;
	}
}
