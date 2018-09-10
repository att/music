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
package org.onap.music.main;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicServiceException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

@WebListener
public class CronJobManager implements ServletContextListener {

    private ScheduledExecutorService scheduler;

    @Override
    public void contextInitialized(ServletContextEvent event) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(new CachingUtil(), 0, 24, TimeUnit.HOURS);
        PreparedQueryObject pQuery = new PreparedQueryObject();
        String consistency = MusicUtil.EVENTUAL;
        pQuery.appendQueryString("CREATE TABLE IF NOT EXISTS admin.locks ( lock_id text PRIMARY KEY, ctime text)");
        try {
            ResultType result = MusicCore.nonKeyRelatedPut(pQuery, consistency);
        } catch (MusicServiceException e1) {
            e1.printStackTrace();
        }
        
        pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select * from admin.locks");
            try {
                ResultSet rs = MusicCore.get(pQuery);
                Iterator<Row> it = rs.iterator();
                StringBuilder deleteKeys = new StringBuilder();
                Boolean expiredKeys = false;
                while (it.hasNext()) {
                    Row row = (Row) it.next();
                    String id = row.getString("lock_id");
                    long ctime = Long.parseLong(row.getString("ctime"));
                    if(System.currentTimeMillis() >= ctime + 24 * 60 * 60 * 1000) {
                        expiredKeys = true;
                        String new_id = id.substring(1);
                        MusicCore.deleteLock(new_id);
                        deleteKeys.append(id).append(",");
                    }
                    else {
                        MusicUtil.zkNodeMap.put(id, ctime);
                    }
                };
                if(expiredKeys) {
                    deleteKeys.deleteCharAt(deleteKeys.length()-1);
                    deleteKeysFromDB(deleteKeys);
               }
            } catch (MusicServiceException e) {
                e.printStackTrace();
            } catch (MusicLockingException e) {
                e.printStackTrace();
       }
       
      //Zookeeper cleanup
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Iterator<Entry<String, Long>> it = MusicUtil.zkNodeMap.entrySet().iterator();
                StringBuilder deleteKeys = new StringBuilder();
                Boolean expiredKeys = false;
                while (it.hasNext()) {
                    Map.Entry<String, Long> pair = (Map.Entry<String, Long>)it.next();
                    long ctime = pair.getValue();
                   if (System.currentTimeMillis() >= ctime + 24 * 60 * 60 * 1000) {
                       try {
                           expiredKeys = true;
                           String id = pair.getKey();
                           deleteKeys.append("'").append(id).append("'").append(",");
                           MusicCore.deleteLock(id.substring(1));
                           MusicUtil.zkNodeMap.remove(id);
                           
                       } catch (MusicLockingException e) {
                          e.printStackTrace();
                       }
                   }
                }
                if(expiredKeys) {
                    deleteKeys.deleteCharAt(deleteKeys.length()-1);
                    deleteKeysFromDB(deleteKeys);
               }
            }
        } , 0, 24, TimeUnit.HOURS);
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        scheduler.shutdownNow();
    }
    
    public void deleteKeysFromDB(StringBuilder deleteKeys) {
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "DELETE FROM admin.locks WHERE lock_id IN ("+deleteKeys+")");
        try {
            MusicCore.nonKeyRelatedPut(pQuery, "eventual");
        } catch (Exception e) {
              e.printStackTrace();
        }
    }

}
