package org.openecomp.sdnc.sli.resource.dblib;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetProvider;

public class DBResourceManager {
	public static final String DB_CONNECTION = "jdbc:mdbc:file:/tmp/stresstest";		// "jdbc:h2:mem:db1";
	@SuppressWarnings("unused")
	private Properties p;
	private Queue<Connection> conns;

	private DBResourceManager(Properties p) {
		this.p = p;
		this.conns = new LinkedBlockingQueue<Connection>();
	}
	public static DBResourceManager create(Properties props) throws Exception {
		DBResourceManager dbmanager = new DBResourceManager(props);
		return dbmanager;
	}
	public Connection getConnection() throws SQLException {
		if (conns.size() > 0) {
			return conns.remove();
		} else {
			Properties driver_info = new Properties();
			return DriverManager.getConnection(DB_CONNECTION, driver_info);
		}
	}
	public void cleanUp() {
		try {
			while (conns.size() > 0) {
				Connection conn = conns.remove();
				conn.close();
			}
		} catch (SQLException e) {
		}
	}
	public boolean isActive() {
		return true;
	}
	public boolean writeData(String statement, List<String> arguments, String preferredDS) throws SQLException {
		Connection conn = getConnection();
		PreparedStatement ps =  conn.prepareStatement(statement);
        for (int i = 1; i <= arguments.size(); i++) {
        	ps.setObject(i, arguments.get(i-1));
        }
        ps.executeUpdate();
        ps.close();
        conns.add(conn);
		return true;
	}
	public CachedRowSet getData(String statement, List<String> arguments, String preferredDS) throws SQLException {
		CachedRowSet data = null;
		ResultSet rs = null;
		Connection conn = null;
		try {
			data = RowSetProvider.newFactory().createCachedRowSet();
			conn = getConnection();
			PreparedStatement ps = conn.prepareStatement(statement);
			if(arguments != null) {
				for (int i = 0; i < arguments.size(); i++) {
					ps.setObject(i+1, arguments.get(i));
				}
			}
			rs = ps.executeQuery();
			data.populate(rs);
		} catch (Throwable exc) {
			throw (SQLException)exc;
		} finally {
			if (conn != null)
				conns.add(conn);
		}
		return data;
	}
	CachedDataSource findMaster() throws Exception {
		return new CachedDataSource();
	}
}
