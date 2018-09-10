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

package org.onap.music.unittests.jsonobjects;

import static org.junit.Assert.*;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.JsonTable;

public class JsonTableTest {

    JsonTable jt = null;
    
    @Before
    public void init() {
        jt = new JsonTable();
    }
    
    @Test
    public void testGetConsistencyInfo() {
        Map<String, String> mapSs = new HashMap<>();
        mapSs.put("k1", "one");
        jt.setConsistencyInfo(mapSs);
        assertEquals("one",jt.getConsistencyInfo().get("k1"));
    }

    @Test
    public void testGetProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("k1", "one");
        jt.setProperties(properties);
    }

    @Test
    public void testGetFields() {
        Map<String, String> fields = new HashMap<>();
        fields.put("k1", "one");
        jt.setFields(fields);
        assertEquals("one",jt.getFields().get("k1"));
    }

    @Test
    public void testGetKeyspaceName() {
        String keyspace = "keyspace";
        jt.setKeyspaceName(keyspace);
        assertEquals(keyspace,jt.getKeyspaceName());
    }

    @Test
    public void testGetTableName() {
        String table = "table";
        jt.setTableName(table);
        assertEquals(table,jt.getTableName());
   }

    @Test
    public void testGetSortingKey() {
        String sortKey = "sortkey";
        jt.setSortingKey(sortKey);
        assertEquals(sortKey,jt.getSortingKey());
    }

    @Test
    public void testGetClusteringOrder() {
        String clusteringOrder = "clusteringOrder";
        jt.setClusteringOrder(clusteringOrder);
        assertEquals(clusteringOrder,jt.getClusteringOrder());
    }

    @Test
    public void testGetPrimaryKey() {
        String primaryKey = "primaryKey";
        jt.setPrimaryKey(primaryKey);
        assertEquals(primaryKey,jt.getPrimaryKey());        
    }

}
