package com.att.research.mdbc.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

import com.att.research.mdbc.Driver;

/**
 * This is a basic test which creates some tables, does a few selects, adn runs some joins.
 * It is mainly intended to make sure that no exceptions are thrown in basic operation.
 */
public class BasicTest extends TestCommon {
	private static final String DB_CONNECTION = Driver.PROXY_PREFIX + "mem:db1";
	private static final String KEYSPACE      = "Basic_Test";

	//@Test
	public void test() {
		try {
			Connection connection = getDBConnection(DB_CONNECTION, KEYSPACE, "0");
			assertNotNull(connection);
			System.out.println("GOT conn");
			Statement stmt = connection.createStatement();
			assertNotNull(stmt);
			System.out.println("GOT stmt");

			try {
				connection.setAutoCommit(false);
				stmt.execute("CREATE TABLE IF NOT EXISTS PERSON(ID_ varchar(255), NAME varchar(255), SSN varchar(255), primary key (ID_))");
				stmt.execute("INSERT INTO PERSON(ID_, NAME, SSN) VALUES('1', 'Anju',  '111-22-3333')");
				stmt.execute("INSERT INTO PERSON(ID_, NAME, SSN) VALUES('2', 'Sonia', '111-22-4444')");
				stmt.execute("INSERT INTO PERSON(ID_, NAME, SSN) VALUES('3', 'Asha',  '111-55-6666')");
				dumptable(connection);

				stmt.execute("DELETE FROM PERSON WHERE ID_ = '1'");
				dumptable(connection);

				stmt.execute("UPDATE PERSON SET NAME = 'foobar' WHERE ID_ = '2'");
				dumptable(connection);

				stmt.execute("CREATE TABLE IF NOT EXISTS SONG(ID_ varchar(255), PREF int, ARIA varchar(255), primary key (ID_, PREF))");
				stmt.execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('1', 1, 'Nessun Dorma')");
				stmt.execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('2', 5, 'O mio Bambino Caro')");
				stmt.execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('2', 2, 'Sweet Georgia Brown')");
				stmt.execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('3', 77, 'Mud Flats Blues')");
				stmt.execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('3', 69, 'Me & Mr Jones')");
				ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON AS P, SONG AS S WHERE P.ID_ = S.ID_");
				while (rs.next()) {
					System.out.println("ID_ " + rs.getInt("ID_") + " Name: " + rs.getString("NAME") + " Aria: " + rs.getString("ARIA"));
				}
				rs.close();
				stmt.close();
				connection.commit();
			} catch (Exception e) {
				fail(e.toString());
			} finally {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail(e.toString());
		}
		System.out.println("BasicTest.test OK");
	}

	private void dumptable(Connection connection) throws SQLException {
		Statement stmt = connection.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM PERSON");
		while (rs.next()) {
			System.out.println("ID_ " + rs.getInt("ID_") + " Name " + rs.getString("name"));
		}
		stmt.close();
		System.out.println("--");
	}
}
