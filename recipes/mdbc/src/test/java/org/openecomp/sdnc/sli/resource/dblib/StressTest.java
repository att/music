package org.openecomp.sdnc.sli.resource.dblib;

import static org.junit.Assert.*;

import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.rowset.CachedRowSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anarsoft.vmlens.concurrent.junit.ConcurrentTestRunner;
import com.anarsoft.vmlens.concurrent.junit.ThreadCount;

//@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(ConcurrentTestRunner.class)
public class StressTest {

//	static {
//		System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "INFO");
//		System.setProperty(org.slf4j.impl.SimpleLogger.LOG_FILE_KEY, String.format("ComparativeAnalysisTest-%d.log", System.currentTimeMillis()));
//	}
	private static final Logger LOG = LoggerFactory.getLogger(StressTest.class);
	private static Properties props;
	private static DBResourceManager jdbcDataSource = null;
	@SuppressWarnings("unused")
	private static final int MAX_TREADS = 1;
	@SuppressWarnings("unused")
	private static final int MAX_ITERATIONS = 10;
	
	private final AtomicInteger  count= new AtomicInteger();

	Set<Thread> runningThreads = new HashSet<Thread>();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		props = new Properties();
		URL url = StressTest.class.getResource("/dblib.properties");
		assertNotNull(url);
		LOG.info("Property file is: " + url.toString());
		props.load(url.openStream());

		try {
			jdbcDataSource = DBResourceManager.create(props);
			Connection conn = jdbcDataSource.getConnection();

			// ---------------
			// CREATE TABLE
			String sql =
				"CREATE TABLE IF NOT EXISTS `AIC_SITE` (" + 
					"`name` varchar(100) DEFAULT NULL, "+
					"`aic_site_id` varchar(100) NOT NULL, "+
					"`vcenter_url` varchar(200) DEFAULT NULL, "+
					"`vcenter_username` varchar(40) DEFAULT NULL, "+
					"`vcenter_passwd` varchar(255) DEFAULT NULL, "+
					"`city` varchar(100) DEFAULT NULL, "+
					"`state` varchar(2) DEFAULT NULL, "+
					"`operational_status` varchar(20) DEFAULT NULL, "+
					"`oam_gateway_addr` varchar(20) DEFAULT '', "+
					"PRIMARY KEY (`aic_site_id`) "+
				") ; ";
			Statement stmt = conn.createStatement();
			stmt.execute(sql);
			// ---------------

			conn.close();
		} catch (Throwable exc) {
			LOG.error("", exc);
		}
		assertNotNull(jdbcDataSource);
		if (((DBResourceManager)jdbcDataSource).isActive()){
			LOG.warn( "DBLIB: JDBC DataSource has been initialized.");
		} else {
			LOG.warn( "DBLIB: JDBC DataSource did not initialize successfully.");
		}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		jdbcDataSource.cleanUp();
	}

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {

	}

//	@Test
	public void test01() {
		LOG.info("TEST 1: Verify primary db selection");
		checkPrimaryDatabase();
	}
	
	
	@Test
	@ThreadCount(10)
	public void test0X() {
		int id = count.incrementAndGet();
		
		String siteid = String.format("Councurrent-tester-%02d", id);
		for(int i=0; i<40; i++){
			String site = String.format("%s_%04d", siteid, i);
			insertTestData(site);
			queryTestData(site);
			removeTestData(site);
			try {
				Thread.sleep(0);
			} catch (Exception e) {
				LOG.warn("", e);
			}			
		}
	}

	private void removeTestData(String site) {
		ArrayList<String> delete = new ArrayList<String>();
		delete.add(site);
		try {
			long startTime = System.currentTimeMillis();
			boolean success = jdbcDataSource.writeData("delete from AIC_SITE where aic_site_id=?", delete, null);
			logRequest(site, "DELETE", startTime, System.currentTimeMillis() - startTime);
			assertTrue(success);
		} catch (SQLException e) {
			LOG.warn("", e);
			
		}
	}

	private boolean queryTestData(String site) {
		ArrayList<String> identifier = new ArrayList<String>();
		identifier.add(site);
		try {
			int rowcount = 0;
			long startTime = System.currentTimeMillis();
			CachedRowSet data = jdbcDataSource.getData("select * from AIC_SITE where aic_site_id=?", identifier, null);
			logRequest(site, "QUERY", startTime, System.currentTimeMillis() - startTime);
			while(data.next()) {
				rowcount ++;
			}
			return rowcount!=0;
//			assertTrue(success);
		} catch (SQLException e) {
			LOG.warn("", e);
			return false;
		}
	}


	private void insertTestData(String site) {
		ArrayList<String> data = new ArrayList<String>();
		data.add(site);
		data.add(site);
		data.add("Sample03");
		data.add("Sample04");
		data.add("Sample05");

		boolean success;
		try {
			long startTime = System.currentTimeMillis();
			success = jdbcDataSource.writeData("insert into AIC_SITE (name, aic_site_id, vcenter_url, vcenter_username, vcenter_passwd) values (?,?,?,?,?)", data, null);
			logRequest(site, "INSERT", startTime, System.currentTimeMillis() - startTime);
			assertTrue(success);
		} catch (SQLException e) {
			LOG.warn("", e);
		}
	}

	private void checkPrimaryDatabase() {
		Connection conn = null;
		PreparedStatement statement = null;
		ResultSet rs  = null;
		
		try {
			conn = jdbcDataSource.getConnection();
			statement = conn.prepareStatement("SELECT 1 FROM DUAL");
			rs = statement.executeQuery();
			int value = -1;
			while(rs.next()) {
				value = rs.getInt(1);
			}
			LOG.info("Value returned is: " + value);
			conn.close();
		} catch (SQLException e) {
			LOG.warn("transaction failed", e);
		} finally {
			try {
				if(rs != null) 	{ rs.close();	} 
				if(conn != null){ conn.close();	}
				if(conn != null){ conn.close();	}
			} catch (SQLException e) {
				LOG.warn("transaction failed", e);
			}
		}
		CachedDataSource ds = null;
		try {
			ds = jdbcDataSource.findMaster();
		} catch (Throwable e) {
			LOG.warn("", e);
		} 
		LOG.info("Primary DS is " + ds.getDbConnectionName());
	}
	private static void logRequest(String site, String command, long timestamp, long duration) {
		LOG.info(String.format("%s|%s|%d|%d", site, command, timestamp, duration));
	}	
}
