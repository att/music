package com.att.research.mdbc.mixins;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.json.JSONObject;
import org.json.JSONTokener;

import com.att.research.logging.EELFLoggerDelegate;
import com.att.research.mdbc.MusicSqlManager;
import com.att.research.mdbc.TableInfo;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

/**
 * This class provides the methods that MDBC needs in order to mirror data to/from an H2 Database instance
 * running on a server.
 *
 * @author Robert P. Eby
 */
public class H2ServerMixin extends H2Mixin {
	public  static final String MIXIN_NAME = "h2server";
	private static final String triggerClassName = H2ServerMixinTriggerHandler.class.getName();
	private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(H2ServerMixin.class);

	private boolean server_tbl_created = false;

	public H2ServerMixin() {
		super();
	}
	public H2ServerMixin(MusicSqlManager msm, String url, Connection conn, Properties info) {
		super(msm, url, conn, info);
	}
	// This is used to generate a unique connId for this connection to the DB.
	@Override
	protected int generateConnID(Connection conn) {
		int rv = (int) System.currentTimeMillis();
		try {
			UUID uuid = UUID.randomUUID();
			Statement stmt = conn.createStatement();
			stmt.execute("CREATE TABLE IF NOT EXISTS MDBC_UNIQUEID (UUID VARCHAR(36) PRIMARY KEY, IX INT AUTO_INCREMENT)");
			stmt.execute("INSERT INTO MDBC_UNIQUEID (UUID) VALUES ('"+uuid+"')");
			ResultSet rs = stmt.executeQuery("SELECT IX FROM MDBC_UNIQUEID WHERE UUID = '"+uuid+"'");
			if (rs.next()) {
				rv = rs.getInt("IX");
			}
			stmt.close();
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"generateConnID: problem creating/using the MDBC_UNIQUEID table!"+e);
		}
		return rv;
	}
	/**
	 * Get the name of this DBnterface mixin object.
	 * @return the name
	 */
	@Override
	public String getMixinName() {
		return MIXIN_NAME;
	}
	/**
	 * This method should create triggers on the database for each row after every INSERT,
	 * UPDATE and DELETE, and before every SELECT.
	 * @param tableName this is the table on which triggers are being created.
	 */
	@Override
	public void createSQLTriggers(String tableName) {
		// Don't create triggers for the table the triggers write into!!!
		if (tableName.equals(H2ServerMixinTriggerHandler.TRANS_TBL))
			return;
		try {
			if (!server_tbl_created) {
				H2ServerMixinTriggerHandler.createTransTable(dbConnection);
				logger.info(EELFLoggerDelegate.applicationLogger,"Server side dirty table created.");
				
				server_tbl_created = true;
			}

			// Give the triggers a way to find this MSM
			for (String name : getTriggerNames(tableName)) {
				logger.error(EELFLoggerDelegate.errorLogger,"ADD trigger "+name+" to msm_map");
				msm.register(name);
			}
			System.out.println("CREATE TRIGGER IF NOT EXISTS I_"+connId+"_" +tableName+" AFTER INSERT ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS I_"+connId+"_" +tableName+" AFTER INSERT ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS U_"+connId+"_" +tableName+" AFTER UPDATE ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS D_"+connId+"_" +tableName+" AFTER DELETE ON " +tableName+" FOR EACH ROW CALL \""+triggerClassName+"\"");
			// No SELECT trigger on the server, instead we intercept the SELECTS going to the server and preload data
//			executeSQLWrite("CREATE TRIGGER IF NOT EXISTS S_"+connId+"_" +tableName+" BEFORE SELECT ON "+tableName+" CALL \""+triggerClassName+"\"");
			dbConnection.commit();
		} catch (SQLException e) {
			logger.error(EELFLoggerDelegate.errorLogger,"createSQLTriggers: "+e);
		}
	}
	private String[] getTriggerNames(String tableName) {
		return new String[] {
			"I_" + connId + "_" + tableName,	// INSERT trigger
			"U_" + connId + "_" + tableName,	// UPDATE trigger
			"D_" + connId + "_" + tableName		// DELETE trigger
		};
	}
	/**
	 * Code to be run within the DB driver before a SQL statement is executed.  This is where tables
	 * can be synchronized before a SELECT, for those databases that do not support SELECT triggers.
	 * @param sql the SQL statement that is about to be executed
	 * @return keys of rows that are updated during sql query
	 */
	@Override
	public void preStatementHook(final String sql) {
		if (sql == null) {
			return;
		}
		String cmd = sql.trim().toLowerCase();
		if (cmd.startsWith("select")) {
			String[] parts = sql.trim().split(" ");
			Set<String> set = getSQLTableSet();
			for (String part : parts) {
				if (set.contains(part.toUpperCase())) {
					// Found a candidate table name in the SELECT SQL -- update this table
					msm.readDirtyRowsAndUpdateDb(part);
				}
			}
		}
	}
	/**
	 * Code to be run within the DB driver after a SQL statement has been executed.  This is where remote
	 * statement actions can be copied back to Cassandra/MUSIC.
	 * @param sql the SQL statement that was executed
	 */
	@Override
	public void postStatementHook(final String sql) {
		if (sql != null) {
			String[] parts = sql.trim().split(" ");
			String cmd = parts[0].toLowerCase();
			if ("delete".equals(cmd) || "insert".equals(cmd) || "update".equals(cmd)) {
				// copy from DB.MDBC_TRANSLOG where connid == myconnid
				// then delete from MDBC_TRANSLOG
				String sql2 = "SELECT IX, TABLENAME, OP, KEYDATA FROM "+H2ServerMixinTriggerHandler.TRANS_TBL+" WHERE CONNID = "+connId;
				try {
					ResultSet rs = executeSQLRead(sql2);
					Set<Integer> rows = new TreeSet<Integer>();
					while (rs.next()) {
						int ix      = rs.getInt("IX");
						int op      = rs.getInt("OP");
						String tbl  = rs.getString("TABLENAME");
						String keys = rs.getString("KEYDATA");
						JSONObject jo = new JSONObject(new JSONTokener(keys));
						Object[] row = jsonToRow(tbl, jo);
						// copy to cassandra
						if (op ==  H2ServerMixinTriggerHandler.OP_DELETE) {
							msm.deleteFromEntityTableInMusic(tbl, row);
						} else {
							msm.updateDirtyRowAndEntityTableInMusic(tbl, row);
						}
						rows.add(ix);
					}
					if (rows.size() > 0) {
						sql2 = "DELETE FROM "+H2ServerMixinTriggerHandler.TRANS_TBL+" WHERE IX = ?";
						PreparedStatement ps = dbConnection.prepareStatement(sql2);
						logger.info(EELFLoggerDelegate.applicationLogger,"Executing: "+sql2);
						logger.info(EELFLoggerDelegate.applicationLogger,"  For ix = "+rows);
						
						for (int ix : rows) {
							ps.setInt(1, ix);
							ps.execute();
						}
						ps.close();
					}
				} catch (SQLException e) {
					logger.error(EELFLoggerDelegate.errorLogger,"Exception in postStatementHook: "+e);
					e.printStackTrace();
				}
			}
		}
	}
	
	
	@SuppressWarnings("deprecation")
	private Object[] jsonToRow(String tbl, JSONObject jo) {
		TableInfo ti = getTableInfo(tbl);
		Object[] rv = new Object[ti.columns.size()];
		for (int i = 0; i < rv.length; i++) {
			String colname = ti.columns.get(i);
			switch (ti.coltype.get(i)) {
			case Types.BIGINT:
				rv[i] = jo.getLong(colname);
				break;
			case Types.BOOLEAN:
				rv[i] = jo.getBoolean(colname);
				break;
			case Types.BLOB:
				logger.error(EELFLoggerDelegate.errorLogger,"WE DO NOT SUPPORT BLOBS IN H2!! COLUMN NAME="+colname);
				// throw an exception here???
				break;
			case Types.DOUBLE:
				rv[i] = jo.getDouble(colname);
				break;
			case Types.INTEGER:
				rv[i] = jo.getInt(colname);
				break;
			case Types.TIMESTAMP:
				rv[i] = new Date(jo.getString(colname));
				break;
			case Types.VARCHAR:
			default:
				rv[i] = jo.getString(colname);
				break;
			}
		}
		return rv;
	}
	
	@Override
	public void synchronizeData(String tableName) {
		// TODO Auto-generated method stub
		System.out.println("In Synchronize data");
		
		ResultSet rs = null;
		TableInfo ti = getTableInfo(tableName);
		System.out.println("Table info :"+ti);
		String query = "select * from "+tableName;
		System.out.println("query is :"+query);
		
		try {
			 rs = executeSQLRead(query);
			 while(rs.next()) {
				 
				JSONObject jo = new JSONObject();
				if (!getTableInfo(tableName).hasKey()) {
						String musicKey = msm.generatePrimaryKey();;
						jo.put(msm.getMusicDefaultPrimaryKeyName(), musicKey);	
				}
					
				for (String col : ti.columns) {
						jo.put(col, rs.getString(col));
				}
					
				Object[] row = jsonToRow(tableName, jo);
				
				msm.updateDirtyRowAndEntityTableInMusic(tableName, row);
				 
			 }
		} catch (Exception e) {
			System.out.println("sql exception");
		}
		finally {
			try {
				rs.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
}
