package com.att.research.mdbc.test;

import static org.junit.Assert.*;

import java.io.Reader;
import java.io.StringReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.att.research.mdbc.Driver;

/**
 * This test tests a copy of data from DB1 to DB2.  It tests the following H2 data types:
 * VARCHAR, VARBINARY, INTEGER, BOOLEAN, DOUBLE, CLOB, TIMESTAMP.
 */
public class CrossSiteTest extends TestCommon {
	private static final String DB_CONNECTION1 = Driver.PROXY_PREFIX + "mem:db1";
	private static final String DB_CONNECTION2 = Driver.PROXY_PREFIX + "mem:db2";
	private static final String KEYSPACE       = "CrossSite_Test";
	private final static Logger logger = Logger.getLogger(CrossSiteTest.class);

	private Connection db1, db2;

	//@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// drop the keyspace
	}

	//@Before
	public void setUp() throws Exception {
		db1 = getDBConnection(DB_CONNECTION1, KEYSPACE, "0");
		db2 = getDBConnection(DB_CONNECTION2, KEYSPACE, "1");
	}

	//@After
	public void tearDown() throws Exception {
		db1.close();
		db2.close();
	}

	//@Test
	public void testCopyOneToTwo() {
		String sql = "CREATE TABLE IF NOT EXISTS DATA(KEY VARCHAR(255), PRIMARY KEY (KEY))";
		createTable(sql);

		// Put data in DB1
		try {
			Statement s = db1.createStatement();
			s.execute("INSERT INTO DATA(KEY) VALUES('AAA')");
			s.execute("INSERT INTO DATA(KEY) VALUES('BBB')");
			s.execute("INSERT INTO DATA(KEY) VALUES('CCC')");
			s.execute("INSERT INTO DATA(KEY) VALUES('DDD')");
			db1.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM DATA");
			if (rs.next()) {
				int n = rs.getInt(1);
				assertEquals(4, n);
			} else {
				fail("SELECT COUNT(*) produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		// Delete a row
		try {
			Statement s = db1.createStatement();
			s.execute("DELETE FROM DATA WHERE KEY = 'CCC'");
			db1.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Recheck
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM DATA");
			if (rs.next()) {
				int n = rs.getInt(1);
				assertEquals(3, n);
			} else {
				fail("SELECT COUNT(*) produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		System.out.println("CrossSiteTest.testCopyOneToTwo OK");
	}

	//@Test
	public void testCopyWithPreparedStatement() {
		String sql = "CREATE TABLE IF NOT EXISTS DATA2(KEY VARCHAR(255), PRIMARY KEY (KEY))";
		createTable(sql);

		// Put data in DB1
		try {
			Statement s = db1.createStatement();
			PreparedStatement ps =  db1.prepareStatement("INSERT INTO DATA2(KEY) VALUES(?)");
			for (String v : new String[] { "WWW", "XXX", "YYY", "ZZZ" } ) {
				ps.setString(1, v);
				ps.execute();
			}
			db1.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM DATA2");
			if (rs.next()) {
				int n = rs.getInt(1);
				assertEquals(4, n);
			} else {
				fail("SELECT COUNT(*) produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		System.out.println("CrossSiteTest.testCopyWithPreparedStatement OK");
	}

	//@Test
	public void testDataTypes() {
		String sql = "CREATE TABLE IF NOT EXISTS DATATYPES(KEY VARCHAR(255), I1 INTEGER, B1 BOOLEAN, D1 DOUBLE, S1 VARCHAR, PRIMARY KEY (KEY))";
		createTable(sql);

		String key  = "ThIs Is ThE KeY";
		String key2 = "ThIs Is another KeY";
		String s1 = "The Rain in Spain";
		int i1 = 696969;
		boolean b1 = true;
		double pi = Math.PI;
		double e  = Math.E;

		// Put data in DB1
		try {
			PreparedStatement ps =  db1.prepareStatement("INSERT INTO DATATYPES(KEY, I1, B1, D1, S1) VALUES(?, ?, ?, ?, ?)");
			ps.setString(1, key);
			ps.setInt(2, i1);
			ps.setBoolean(3, b1);
			ps.setDouble(4, pi);
			ps.setString(5, s1);
			ps.execute();

			ps.setString(1, key2);
			ps.setInt(2, 123456);
			ps.setBoolean(3, false);
			ps.setDouble(4, e);
			ps.setString(5, "Fee fi fo fum!");
			ps.execute();
			db1.commit();
			ps.close();
		} catch (Exception ex) {
			fail("1: " + ex.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT * FROM DATATYPES");
			if (rs.next()) {
				assertEquals(key, rs.getString(1));
				assertEquals(i1,  rs.getInt(2));
				assertEquals(b1,  rs.getBoolean(3));
				assertEquals(pi,  rs.getDouble(4), 0.0);
				assertEquals(s1,  rs.getString(5));
			} else {
				fail("SELECT * FROM DATATYPES");
			}
		} catch (Exception ex) {
			logger.error(ex);
			ex.printStackTrace();
			fail("2: " + ex.toString());
		}
		System.out.println("CrossSiteTest.testDataTypes OK");
	}

	//@Test
	public void testIdentityColumn() {
		String sql = "CREATE TABLE IF NOT EXISTS IDENTITYTEST(KEY IDENTITY, S1 VARCHAR, T1 TIMESTAMP, PRIMARY KEY (KEY))";
		createTable(sql);

		String s1  = "ThIs Is ThE IDENTITY test";
		Timestamp ts = new Timestamp(-3535344000L);

		// Put data in DB1
		try {
			PreparedStatement ps =  db1.prepareStatement("INSERT INTO IDENTITYTEST(S1, T1) VALUES(?, ?)");
			ps.setString(1, s1);
			ps.setTimestamp(2, ts);
			ps.execute();
			db1.commit();
			ps.close();
		} catch (Exception ex) {
			fail("testIdentity 1: " + ex.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT * FROM IDENTITYTEST");
			if (rs.next()) {
				assertEquals(s1, rs.getString("s1"));
				assertEquals(ts, rs.getTimestamp("t1"));
			} else {
				fail("SELECT * FROM DATATYPES");
			}
		} catch (Exception ex) {
			logger.error(ex);
			ex.printStackTrace();
			fail("testIdentity 2: " + ex.toString());
		}
		System.out.println("CrossSiteTest.testIdentityColumn OK");
	}

	//@Test
	public void testBLOBColumn() {
		String sql = "CREATE TABLE IF NOT EXISTS BLOBTEST (KEY VARCHAR, V1 VARBINARY, C1 CLOB, PRIMARY KEY (KEY))";// add
		createTable(sql);

		String key = "BLOB test";
		byte[] v1 = new byte[4096];
		new Random().nextBytes(v1);
		String constitution =
			"We the People of the United States, in Order to form a more perfect Union, establish Justice, insure domestic Tranquility, provide for the common defense, promote the "+
			"general Welfare, and secure the Blessings of Liberty to ourselves and our Posterity, do ordain and establish this Constitution for the United States of America."+
			"Section 1"+
			"All legislative Powers herein granted shall be vested in a Congress of the United States, which shall consist of a Senate and House of Representatives."+
			""+
			"Section 2"+
			"1: The House of Representatives shall be composed of Members chosen every second Year by the People of the several States, and the Electors in each State shall "+
			"have the Qualifications requisite for Electors of the most numerous Branch of the State Legislature."+
			""+
			"2: No Person shall be a Representative who shall not have attained to the Age of twenty five Years, and been seven Years a Citizen of the United States, "+
			"and who shall not, when elected, be an Inhabitant of that State in which he shall be chosen."+
			""+
			"3: Representatives and direct Taxes shall be apportioned among the several States which may be included within this Union, according to their respective Numbers, which shall be determined "+
			"by adding to the whole Number of free Persons, including those bound to Service for a Term of Years, and excluding Indians not taxed, three fifths of all other Persons. "+
			"2  The actual Enumeration shall be made within three Years after the first Meeting of the Congress of the United States, and within every subsequent Term of ten Years, in such Manner as "+
			"they shall by Law direct. The Number of Representatives shall not exceed one for every thirty Thousand, but each State shall have at Least one Representative; and until such enumeration "+
			"shall be made, the State of New Hampshire shall be entitled to chuse three, Massachusetts eight, Rhode-Island and Providence Plantations one, Connecticut five, New-York six, New Jersey four, "+
			"Pennsylvania eight, Delaware one, Maryland six, Virginia ten, North Carolina five, South Carolina five, and Georgia three."+
			""+
			"4: When vacancies happen in the Representation from any State, the Executive Authority thereof shall issue Writs of Election to fill such Vacancies."+
			""+
			"5: The House of Representatives shall chuse their Speaker and other Officers; and shall have the sole Power of Impeachment."+
			"etc., etc. ...";
		Reader c1 = new StringReader(constitution);

		// Put data in DB1
		try {
			CallableStatement ps =  db1.prepareCall("INSERT INTO BLOBTEST(KEY, V1, C1) VALUES (?, ?, ?)");
			ps.setString(1, key);
			ps.setBytes(2, v1);
			ps.setClob(3, c1);
			ps.execute();
			db1.commit();
			ps.close();
		} catch (Exception ex) {
			ex.printStackTrace();
			fail("testBLOBColumn 1: " + ex.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT * FROM BLOBTEST");
			if (rs.next()) {
				String v1s = new String(v1);
				assertEquals(key, rs.getString("key"));
				assertEquals(v1s, new String(rs.getBytes("v1")));
				assertEquals(constitution, new String(rs.getBytes("c1")));
			} else {
				fail("SELECT * FROM BLOBTEST");
			}
		} catch (Exception ex) {
			logger.error(ex);
			ex.printStackTrace();
			fail("testBLOBColumn 2: " + ex.toString());
		}
		System.out.println("CrossSiteTest.testBLOBColumn OK");
	}

	//@Test
	public void testSecondaryIndex() {
		String sql = "CREATE TABLE IF NOT EXISTS ARTISTS (ARTIST VARCHAR, GENRE VARCHAR, AGE INT, PRIMARY KEY (ARTIST))";
		createTable(sql);

		// Put data in DB1
		try {
			Statement s = db1.createStatement();
			s.execute("INSERT INTO ARTISTS(ARTIST, GENRE, AGE) VALUES('Anne-Sophie', 'classical', 53)");
			s.execute("INSERT INTO ARTISTS(ARTIST, GENRE, AGE) VALUES('Dizz', 'jazz', 99)");
			s.execute("INSERT INTO ARTISTS(ARTIST, GENRE, AGE) VALUES('Esperanza', 'jazz', 32)");
			s.execute("INSERT INTO ARTISTS(ARTIST, GENRE, AGE) VALUES('Miles', 'jazz', 90)");
			s.execute("INSERT INTO ARTISTS(ARTIST, GENRE, AGE) VALUES('Yo-yo', 'classical', 61)");
			s.execute("CREATE INDEX BYGENRE on ARTISTS(GENRE)");
			db1.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ARTISTS WHERE GENRE = 'jazz'");
			if (rs.next()) {
				int n = rs.getInt(1);
				assertEquals(3, n);
			} else {
				fail("SELECT COUNT(*) produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		// Delete a row
		try {
			Statement s = db1.createStatement();
			s.execute("DELETE FROM ARTISTS WHERE ARTIST = 'Miles'");
			db1.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Recheck
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM ARTISTS WHERE GENRE = 'jazz'");
			if (rs.next()) {
				int n = rs.getInt(1);
				assertEquals(2, n);
			} else {
				fail("SELECT COUNT(*) produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		System.out.println("CrossSiteTest.testSecondaryIndex OK");
	}

	//@Test
	public void testUpdate() {
		String sql = "CREATE TABLE IF NOT EXISTS UPDATETEST(KEY VARCHAR(255), OTHER VARCHAR(255), PRIMARY KEY (KEY))";
		createTable(sql);

		// Put data in DB1
		try {
			Statement s = db1.createStatement();
			s.execute("INSERT INTO UPDATETEST(KEY, OTHER) VALUES('foo', 'bar')");
			s.execute("INSERT INTO UPDATETEST(KEY, OTHER) VALUES('bar', 'nixon')");
			db1.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Get data in DB2
		logger.info("    Get data in DB2");
		try {
			Statement s = db2.createStatement();
			ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM UPDATETEST");
			if (rs.next()) {
				int n = rs.getInt(1);
				assertEquals(2, n);
			} else {
				fail("SELECT COUNT(*) produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		// Update a row
		try {
			Statement s = db2.createStatement();
			s.execute("UPDATE UPDATETEST SET OTHER = 'obama' WHERE KEY = 'bar'");
			db2.commit();
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
		// Recheck
		logger.info("    Get data in DB2");
		try {
			Statement s = db1.createStatement();
			ResultSet rs = s.executeQuery("SELECT OTHER FROM UPDATETEST WHERE KEY = 'bar'");
			if (rs.next()) {
				String str = rs.getString("OTHER");
				assertEquals("obama", str);
			} else {
				fail("SELECT OTHER produced no result");
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
		System.out.println("CrossSiteTest.testUpdate OK");
	}

	private void createTable(String sql) {
		try {
			for (Connection db : new Connection[] { db1, db2 }) {
				logger.info("    start: "+db);
				Statement s = db.createStatement();
				s.execute(sql);
				db.commit();
				s.close();
				logger.info("    Tables created");
			}
		} catch (Exception e) {
			fail(e.toString());
		}
	}
}
