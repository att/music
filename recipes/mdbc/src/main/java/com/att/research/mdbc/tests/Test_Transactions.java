package com.att.research.mdbc.tests;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Test that transactions work between the original DB, and replica DBs.
 *
 * @author Robert Eby
 */
public class Test_Transactions extends Test {
	private final String TBL = "TRANSTEST";

	@Override
	public List<String> run(JSONObject config) {
		List<String> msgs = new ArrayList<String>();
		JSONArray connections = config.getJSONArray("connections");
		Connection[] conn = new Connection[connections.length()];
		Statement[] stmt = new Statement[conn.length];
		try {
			for (int i = 0; i < conn.length; i++) {
				conn[i] = getDBConnection(buildProperties(config, i));
				assertNotNull(conn[i]);
				stmt[i] = conn[i].createStatement();
				assertNotNull(stmt[i]);
			}

			try {
				for (int i = 0; i < conn.length; i++) {
					conn[i].setAutoCommit(true);
					stmt[i].execute("CREATE TABLE IF NOT EXISTS TRANSTEST(ID_ varchar(12), STUFF varchar(255), primary key (ID_))");
					conn[i].setAutoCommit(false);
				}
				stmt[0].execute("INSERT INTO TRANSTEST(ID_, STUFF) VALUES('1', 'CenturyLink Now Under Fire on All Sides For Fraudulent Billing')");
				stmt[0].execute("INSERT INTO TRANSTEST(ID_, STUFF) VALUES('2', 'Netflix Now in Half of All Broadband Households, Study Says')");
				stmt[0].execute("INSERT INTO TRANSTEST(ID_, STUFF) VALUES('3', 'Private Data Of 6 Million Verizon Customers Exposed')");
				assertTableContains(0, conn[0], TBL, "ID_", "1");
				assertTableContains(0, conn[0], TBL, "ID_", "2");
				assertTableContains(0, conn[0], TBL, "ID_", "3");
				for (int i = 1; i < conn.length; i++) {
					assertTableDoesNotContain(i, conn[i], TBL, "ID_", "1");
					assertTableDoesNotContain(i, conn[i], TBL, "ID_", "2");
					assertTableDoesNotContain(i, conn[i], TBL, "ID_", "3");
				}
				conn[0].commit();
				for (int i = 0; i < conn.length; i++) {
					assertTableContains(i, conn[i], TBL, "ID_", "1");
					assertTableContains(i, conn[i], TBL, "ID_", "2");
					assertTableContains(i, conn[i], TBL, "ID_", "3");
				}
				
			} catch (Exception e) {
				msgs.add(e.toString());
			} finally {
				for (int i = 0; i < stmt.length; i++) {
					if (stmt[i] != null)
						stmt[i].close();
				}
				for (int i = 0; i < conn.length; i++) {
					if (conn[i] != null)
						conn[i].close();
				}
			}
		} catch (Exception e) {
			msgs.add(e.toString());
		}
		return msgs;
	}
}
