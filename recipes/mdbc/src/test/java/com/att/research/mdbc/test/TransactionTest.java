package com.att.research.mdbc.test;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.att.research.mdbc.ProxyDriver;

public class TransactionTest extends TestCommon {
	private static final String DB_CONNECTION1 = ProxyDriver.PROXY_PREFIX + "mem:db1";
	private static final String DB_CONNECTION2 = ProxyDriver.PROXY_PREFIX + "mem:db2";
	private static final String KEYSPACE       = "CrossSite_Test";
	private final static Logger logger = Logger.getLogger(CrossSiteTest.class);

	@Test
	public void testWithAutocommitTrue() {
		System.out.println("START TransactionTest.testWithAutocommitTrue");
		Set<String> vals = new HashSet<String>(Arrays.asList("1", "2", "3"));
		Connection db1 = null, db2 = null;
		try {
			db1 = getDBConnection(DB_CONNECTION1, KEYSPACE, "0");
			db2 = getDBConnection(DB_CONNECTION2, KEYSPACE, "1");
			createTable(new Connection[] { db1, db2 });
			db1.setAutoCommit(true);
			insert(db1, vals);
			readcheck(db2, vals);
		} catch (Exception e) {
			fail("Unexpected exception: "+e);
		} finally {
			try {
				if (db1 != null)
					db1.close();
				if (db2 != null)
					db2.close();
			} catch (SQLException e) {
				// ignore
			}
		}
	}
	@Test
	public void testCommit() {
		System.out.println("START TransactionTest.testCommit");
		Set<String> vals = new HashSet<String>(Arrays.asList("1", "2", "3", "4"));
		Set<String> val2 = new HashSet<String>(Arrays.asList("1", "2", "4"));
		Connection db1 = null, db2 = null;
		try {
			db1 = getDBConnection(DB_CONNECTION1, KEYSPACE, "0");
			db2 = getDBConnection(DB_CONNECTION2, KEYSPACE, "1");
			createTable(new Connection[] { db1, db2 });
			db1.setAutoCommit(false);
			insert(db1, vals);
			delete(db1, new HashSet<String>(Arrays.asList("3")));
			readcheck(db1, val2);
			readcheck(db2, new HashSet<String>());
			db1.commit();
			readcheck(db2, val2);
		} catch (Exception e) {
			fail("Unexpected exception: "+e);
		} finally {
			try {
				if (db1 != null)
					db1.close();
				if (db2 != null)
					db2.close();
			} catch (SQLException e) {
				// ignore
			}
		}
	}
	@Test
	public void testRollback() {
		System.out.println("START TransactionTest.testRollback");
		Set<String> vals = new HashSet<String>(Arrays.asList("1", "2", "3", "4"));
		Connection db1 = null, db2 = null;
		try {
			db1 = getDBConnection(DB_CONNECTION1, KEYSPACE, "0");
			db2 = getDBConnection(DB_CONNECTION2, KEYSPACE, "1");
			createTable(new Connection[] { db1, db2 });
			db1.setAutoCommit(false);
			insert(db1, vals);
			readcheck(db1, vals);
			readcheck(db2, new HashSet<String>());
			db1.rollback();
			readcheck(db1, new HashSet<String>());
			readcheck(db2, new HashSet<String>());
		} catch (Exception e) {
			fail("Unexpected exception: "+e);
		} finally {
			try {
				if (db1 != null)
					db1.close();
				if (db2 != null)
					db2.close();
			} catch (SQLException e) {
				// ignore
			}
		}
	}
	private void createTable(Connection[] c) {
		try {
			for (Connection db : c) {
				logger.info("    start: "+db);
				Statement s = db.createStatement();
				s.execute("CREATE TABLE IF NOT EXISTS TRANSTEST(KEY VARCHAR(255), PRIMARY KEY (KEY))");
				s.close();
				logger.info("    Tables created");
			}
		} catch (Exception e) {
			fail(e.toString());
		}
	}
	private void insert(Connection db, Set<String> vals) {
		// Put data in DB1
		try {
			Statement s = db.createStatement();
			for (String v : vals)
				s.execute("INSERT INTO TRANSTEST(KEY) VALUES('"+v+"')");
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
	}
	private void delete(Connection db, Set<String> vals) {
		// Put data in DB1
		try {
			Statement s = db.createStatement();
			for (String v : vals)
				s.execute("DELETE FROM TRANSTEST WHERE KEY = '"+v+"'");
			s.close();
		} catch (Exception e) {
			fail("1: " + e.toString());
		}
	}
	private void readcheck(Connection db, Set<String> vals) {
		try {
			Statement s = db.createStatement();
			ResultSet rs = s.executeQuery("SELECT * FROM TRANSTEST");
			Set<String> newset = new HashSet<String>();
			while (rs.next()) {
				String tmp = rs.getString(1);
				newset.add(tmp);
			}
			if (vals.size() != newset.size()) {
				fail("wrong number of elements, expected "+vals.size()+" got "+newset.size());
			}
			for (String t : vals) {
				if (!newset.contains(t))
					fail("missing element: "+t);
			}
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
			fail("2: " + e.toString());
		}
	}
}
