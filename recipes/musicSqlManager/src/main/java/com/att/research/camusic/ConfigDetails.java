package com.att.research.camusic;

public class ConfigDetails {
	static final String myId="0";
	static final String[] allReplicaIds = {"0"};
    static final String DB_DRIVER = "org.h2.Driver";
    static final String DB_CONNECTION = "jdbc:h2:~/data/test;AUTO_SERVER=TRUE;DATABASE_EVENT_LISTENER="+DbEventTriggerHandler.class.getName();
    static final String DB_USER = "";
    static final String DB_PASSWORD = "";
	static final String musicAddress="localhost";
	static final String triggerClassName = DbOperationTriggerHandler.class.getName();
	static final String primaryKeyName="ID_";//todo: get it automatically..

}
