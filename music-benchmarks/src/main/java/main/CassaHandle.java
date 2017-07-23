/*
 * 
This licence applies to all files in this repository unless otherwise specifically
stated inside of the file. 

 ---------------------------------------------------------------------------
   Copyright (c) 2016 AT&T Intellectual Property

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at:

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 ---------------------------------------------------------------------------

 */
package main;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class CassaHandle {
	private Session session;
	private Cluster cluster;

	public CassaHandle(){
		connectToCassaCluster();
	}

	public CassaHandle(String remoteIp){
		connectToCassaCluster(remoteIp);
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
	
	private void connectToCassaCluster(){
		Iterator<String> it = getAllPossibleLocalIps().iterator();
		String address= "localhost";
//		logger.debug("Connecting to cassa cluster: Iterating through possible ips:"+getAllPossibleLocalIps());
		while(it.hasNext()){
			try {
				cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
				//cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(Integer.MAX_VALUE);
				Metadata metadata = cluster.getMetadata();
//				logger.debug("Connected to cassa cluster "+metadata.getClusterName()+" at "+address);
/*				for ( Host host : metadata.getAllHosts() ) {
					System.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
							host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
							
				}*/
				session = cluster.connect();
				
				break;
			} catch (NoHostAvailableException e) {
				address= it.next();
			} 
		}
	}
	
	public  ArrayList<String> getAllNodePublicIps(){
		Metadata metadata = cluster.getMetadata();
		ArrayList<String> nodePublicIps = new ArrayList<String>();
		for ( Host host : metadata.getAllHosts() ) {
			nodePublicIps.add(host.getBroadcastAddress().getHostAddress());
		}
		return nodePublicIps;
	}
	
	private void connectToCassaCluster(String address){	
		cluster = Cluster.builder().withPort(9042).addContactPoint(address).build();
		Metadata metadata = cluster.getMetadata();
/*		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datacenter: %s; Host broadcast: %s; Rack: %s\n",
					host.getDatacenter(), host.getBroadcastAddress(), host.getRack());
		}*/
		session = cluster.connect();
	}

	
	public void update(){
		String job = "directCassa"+System.currentTimeMillis();
		String query = "UPDATE BenchmarksKeySpace.BmEmployees SET  job='"+job+"' where name='emp0';";
		session.execute(query);
	}
	
	public void close(){
		session.close();
		cluster.close();
	}
	
	public static void main(String[] args){
		CassaHandle csHandle = new CassaHandle();
		csHandle.update();
		csHandle.close();
	}
	

}
