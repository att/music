/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.eelf.healthcheck;

import java.util.UUID;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;

import com.datastax.driver.core.ConsistencyLevel;

/**
 * @author inam
 *
 */
public class MusicHealthCheck {

	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);

	private String cassandrHost;
	private String zookeeperHost;

	public String getCassandraStatus(String consistency) {
		logger.info(EELFLoggerDelegate.applicationLogger, "Getting Status for Cassandra");
		
		boolean result = false;
		try {
			result = getAdminKeySpace(consistency);
		} catch(Exception e) {
			if(e.getMessage().toLowerCase().contains("unconfigured table healthcheck")) {
				System.out.println("Creating table....");
				boolean ksresult = createKeyspace();
				if(ksresult)
					try {
						result = getAdminKeySpace(consistency);
					} catch (MusicServiceException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
			} else {
				return "One or more nodes are down or not responding.";
			}
		}
		if (result) {
			return "ACTIVE";
		} else {
			logger.info(EELFLoggerDelegate.applicationLogger, "Cassandra Service is not responding");
			return "INACTIVE";
		}
	}

	private Boolean getAdminKeySpace(String consistency) throws MusicServiceException {


		PreparedQueryObject pQuery = new PreparedQueryObject();
		pQuery.appendQueryString("insert into admin.healthcheck (id) values (?)");
		pQuery.addValue(UUID.randomUUID());
			ResultType rs = MusicCore.nonKeyRelatedPut(pQuery, consistency);
			System.out.println(rs);
			if (rs != null) {
				return Boolean.TRUE;
			} else {
				return Boolean.FALSE;
			}


	}
	
	private boolean createKeyspace() {
		PreparedQueryObject pQuery = new PreparedQueryObject();
		pQuery.appendQueryString("CREATE TABLE admin.healthcheck (id uuid PRIMARY KEY)");
		ResultType rs = null ;
		try {
			rs = MusicCore.nonKeyRelatedPut(pQuery, ConsistencyLevel.ONE.toString());
		} catch (MusicServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(rs.getResult().toLowerCase().contains("success"))
			return true;
		else
			return false;
	}

	public String getCassandrHost() {
		return cassandrHost;
	}

	public void setCassandrHost(String cassandrHost) {
		this.cassandrHost = cassandrHost;
	}

	public String getZookeeperHost() {
		return zookeeperHost;
	}

	public void setZookeeperHost(String zookeeperHost) {
		this.zookeeperHost = zookeeperHost;
	}

}
