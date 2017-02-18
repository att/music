package com.att.research.camusic;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class MusicConnector {
	private Session session;
	private Cluster cluster;
	final static Logger logger = Logger.getLogger(MusicConnector.class);

	protected MusicConnector(){
		//to defeat instantiation since this is a singleton
	}
	
	public MusicConnector(String address){
		connectToCassaCluster(address);
	}

	private ArrayList<String> getAllPossibleLocalIps(){
		ArrayList<String> allPossibleIps = new ArrayList<String>();
		try {
			Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
			while(en.hasMoreElements()){
			    NetworkInterface ni=(NetworkInterface) en.nextElement();
			    Enumeration<InetAddress> ee = ni.getInetAddresses();
			    while(ee.hasMoreElements()) {
			        InetAddress ia= (InetAddress) ee.nextElement();
			        allPossibleIps.add(ia.getHostAddress());
			    }
			 }
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return allPossibleIps;
	}
	
	public Session getSession(){
		return session;
	}
	
	public void close(){
		session.close();
		cluster.close();
	}
	
	private void connectToCassaCluster(String address){
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		logger.info("Iterating through possible ips:"+getAllPossibleLocalIps());
		while(it.hasNext()){
			try {
				cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
				//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
				Metadata metadata = cluster.getMetadata();
/*				logger.info("Connected to cluster: %s\n", 
						metadata.getClusterName());
				for ( Host host : metadata.getAllHosts() ) {
					System.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
							host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
				}*/
				logger.info("Connected to cluster:"+metadata.getClusterName()+" at address:"+address);
				session = cluster.connect();
				break;
			} catch (NoHostAvailableException e) {
				address= it.next();
			} 
		}
	}
}
