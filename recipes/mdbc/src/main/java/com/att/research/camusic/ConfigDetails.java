package com.att.research.camusic;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

public class ConfigDetails {
	static  String myId;
	static  String[] allReplicaIds;
    static  String dbDriver = "org.h2.Driver";
    static  String dbConnection;
    static  String dbUser;
    static  String dbPassword;
	static  String musicAddress;
	static  String triggerClassName = DbOperationTriggerHandler.class.getName();
	static  String primaryKeyName="ID_";//todo: get it automatically..
	final static Logger logger = Logger.getLogger(ConfigDetails.class);

	public static void main(String[] args){
		Set<String> a= new HashSet<String>();
		a.add("a");
		a.add("bi");
		
		Set<String> b= new HashSet<String>();
		b.add("b");
		b.add("a");
		
		System.out.println(a.equals(b));
		
	
	}

	public static void populate(){
		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("config.properties");

			// load a properties file
			prop.load(input);

			// get the property values
			myId = prop.getProperty("myId");
			logger.info("myId="+myId);
			
			allReplicaIds = prop.get("allReplicaIds").toString().split("#");
			String allReps ="";
			for(int i=0; i < allReplicaIds.length;++i)
				allReps = allReps + allReplicaIds[i]+" ";
			
			logger.info("allReplicaIds="+allReps);

			String DB_URL = prop.getProperty("dbUrl");
			dbConnection = DB_URL+";AUTO_SERVER=TRUE;MVCC=true;DATABASE_EVENT_LISTENER="+DbEventTriggerHandler.class.getName();
		//	 dbConnection = DB_URL+";MULTI_THREADED=true;AUTO_SERVER=TRUE";
			logger.info("dbConnection URL="+dbConnection);

		    dbUser =  prop.getProperty("dbUser");
			logger.info("db user name="+dbUser);

		    dbPassword =  prop.getProperty("dbPassword");
			logger.info("db password="+dbUser);

		    musicAddress=prop.getProperty("musicAddress");
			logger.info("music address="+musicAddress);

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		
	}

}
