package com.att.research.mdbc.tests;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Test that DELETEs work on the original DB, and are correctly copied to replica DBs.
 *
 * @author Robert Eby
 */
public class Test_Delete extends Test {
	private final String TBL = "DELTABLE";

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
					stmt[i].execute("CREATE TABLE IF NOT EXISTS DELTABLE(ID_ varchar(255), RANDOMTXT varchar(255), primary key (ID_))");
				}
				stmt[0].execute("INSERT INTO DELTABLE(ID_, RANDOMTXT) VALUES('1', 'Everything''s Negotiable Except Cutting Medicaid')");
				stmt[0].execute("INSERT INTO DELTABLE(ID_, RANDOMTXT) VALUES('2', 'Can a Sideways Elevator Help Designers Build Taller Skyscrapers?')");
				stmt[0].execute("INSERT INTO DELTABLE(ID_, RANDOMTXT) VALUES('3', 'Can a Bernie Sanders Ally Win the Maryland Governor''s Mansion?')");
				for (int i = 0; i < conn.length; i++) {
					assertTableContains(i, conn[i], TBL, "ID_", "1");
					assertTableContains(i, conn[i], TBL, "ID_", "2");
					assertTableContains(i, conn[i], TBL, "ID_", "3");
				}

				stmt[0].execute("DELETE FROM DELTABLE WHERE ID_ = '1'");
				for (int i = 0; i < conn.length; i++) {
					assertTableDoesNotContain(i, conn[i], TBL, "ID_", "1");
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
