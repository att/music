package com.att.research.mdbc.tests;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Test that INSERTs work to the original DB, and are correctly copied to replica DBs.
 *
 * @author Robert Eby
 */
public class Test_Insert extends Test {
	private final String PERSON = "PERSON";
	private final String SONG   = "SONG";

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
					stmt[i].execute("CREATE TABLE IF NOT EXISTS PERSON(ID_ varchar(255), NAME varchar(255), SSN varchar(255), primary key (ID_))");
				}
				stmt[0].execute("INSERT INTO PERSON(ID_, NAME, SSN) VALUES('1', 'Zaphod', '111-22-3333')");
				stmt[0].execute("INSERT INTO PERSON(ID_, NAME, SSN) VALUES('2', 'Ripley', '444-55-6666')");
				stmt[0].execute("INSERT INTO PERSON(ID_, NAME, SSN) VALUES('3', 'Spock',  '777-88-9999')");
				for (int i = 0; i < conn.length; i++) {
					assertTableContains(i, conn[i], PERSON, "ID_", "1");
					assertTableContains(i, conn[i], PERSON, "ID_", "2");
					assertTableContains(i, conn[i], PERSON, "ID_", "3");
				}

				stmt[0].execute("UPDATE PERSON SET NAME = 'Jabba' WHERE ID_ = '2'");
				for (int i = 0; i < conn.length; i++) {
					ResultSet rs = getRow(conn[i], PERSON, "ID_", "2");
					if (rs.next()) {
						String v = rs.getString("NAME");
						if (!v.equals("Jabba"))
							throw new Exception("Table PERSON, row with ID_ = '2' was not updated.");
					} else {
						throw new Exception("Table PERSON does not have a row with ID_ = '2'");
					}
					rs.close();
				}

				for (int i = 0; i < conn.length; i++) {
					stmt[i].execute("CREATE TABLE IF NOT EXISTS SONG(ID_ varchar(255), PREF int, ARIA varchar(255), primary key (ID_, PREF))");
				}
				stmt[0].execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('1', 1, 'Nessun Dorma')");
				stmt[0].execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('2', 5, 'O mio Bambino Caro')");
				stmt[0].execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('2', 2, 'Sweet Georgia Brown')");
				stmt[0].execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('3', 77, 'Mud Flats Blues')");
				stmt[0].execute("INSERT INTO SONG(ID_, PREF, ARIA) VALUES('3', 69, 'Me & Mr Jones')");
				for (int i = 0; i < conn.length; i++) {
					assertTableContains(i, conn[i], SONG, "ID_", "1", "PREF", 1);
					assertTableContains(i, conn[i], SONG, "ID_", "2", "PREF", 5);
					assertTableContains(i, conn[i], SONG, "ID_", "2", "PREF", 2);
					assertTableContains(i, conn[i], SONG, "ID_", "3", "PREF", 77);
					assertTableContains(i, conn[i], SONG, "ID_", "3", "PREF", 69);
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
