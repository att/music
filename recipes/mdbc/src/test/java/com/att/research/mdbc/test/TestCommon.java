package com.att.research.mdbc.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.att.research.mdbc.ProxyDriver;
import com.att.research.mdbc.mixins.CassandraMixin;

public class TestCommon {
	public static final String DB_DRIVER   = ProxyDriver.class.getName();
	public static final String DB_USER     = "";
	public static final String DB_PASSWORD = "";

	public Connection getDBConnection(String url, String keyspace, String id) throws SQLException, ClassNotFoundException {
		Class.forName(DB_DRIVER);
		Properties driver_info = new Properties();
		driver_info.put(CassandraMixin.KEY_MY_ID,          id);
		driver_info.put(CassandraMixin.KEY_REPLICAS,       "0,1,2");
		driver_info.put(CassandraMixin.KEY_MUSIC_KEYSPACE, keyspace);
		driver_info.put(CassandraMixin.KEY_MUSIC_ADDRESS,  "localhost");
		driver_info.put("user",     DB_USER);
		driver_info.put("password", DB_PASSWORD);
		return DriverManager.getConnection(url, driver_info);
	}
}
