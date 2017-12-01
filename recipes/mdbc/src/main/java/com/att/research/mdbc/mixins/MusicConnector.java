package com.att.research.mdbc.mixins;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * This class allows for management of the Cassandra Cluster and Session objects.
 *
 * @author Robert P. Eby
 */
public class MusicConnector {
	private final static Logger logger = Logger.getLogger(MusicConnector.class);

	private Session session;
	private Cluster cluster;

	protected MusicConnector() {
		//to defeat instantiation since this is a singleton
	}

	public MusicConnector(String address) {
//		connectToCassaCluster(address);
		connectToMultipleAddresses(address);
	}

	public Session getSession() {
		return session;
	}

	public void close() {
		if (session != null)
			session.close();
		session = null;
		if (cluster != null)
			cluster.close();
		cluster = null;
	}

	private void connectToMultipleAddresses(String address) {
		PoolingOptions poolingOptions =
			new PoolingOptions()
	    	.setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
	    	.setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
		String[] music_hosts = address.split(",");
		if (cluster == null) {
			logger.debug("Initializing MUSIC Client with endpoints "+address);
			cluster = Cluster.builder()
				.withPort(9042)
				.withPoolingOptions(poolingOptions)
				.withoutMetrics()
				.addContactPoints(music_hosts)
				.build();
			Metadata metadata = cluster.getMetadata();
			logger.debug("Connected to cluster:"+metadata.getClusterName()+" at address:"+address);
		}
		session = cluster.connect();
	}

	@SuppressWarnings("unused")
	private void connectToCassaCluster(String address) {
		PoolingOptions poolingOptions =
			new PoolingOptions()
	    	.setConnectionsPerHost(HostDistance.LOCAL,  4, 10)
	    	.setConnectionsPerHost(HostDistance.REMOTE, 2, 4);
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		logger.debug("Iterating through possible ips:"+getAllPossibleLocalIps());
		while (it.hasNext()) {
			try {
				cluster = Cluster.builder()
					.withPort(9042)
					.withPoolingOptions(poolingOptions)
					.withoutMetrics()
					.addContactPoint(address)
					.build();
				//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
				Metadata metadata = cluster.getMetadata();
				logger.debug("Connected to cluster:"+metadata.getClusterName()+" at address:"+address);
				session = cluster.connect();
				break;
			} catch (NoHostAvailableException e) {
				address = it.next();
			}
		}
	}

	private List<String> getAllPossibleLocalIps(){
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
			e.printStackTrace();
		}
		return allPossibleIps;
	}
}
