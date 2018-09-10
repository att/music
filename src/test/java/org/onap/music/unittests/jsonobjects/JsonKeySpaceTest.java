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
import org.onap.music.datastore.jsonobjects.JsonKeySpace;

public class JsonKeySpaceTest {

    JsonKeySpace jk = null;
    
    
    @Before
    public void init() {
        jk = new JsonKeySpace();
    }
                    
                
    
    @Test
    public void testGetConsistencyInfo() {
        Map<String, String> mapSs = new HashMap<>();
        mapSs.put("k1", "one");
        jk.setConsistencyInfo(mapSs);
        assertEquals("one",jk.getConsistencyInfo().get("k1"));
    }

    @Test
    public void testGetReplicationInfo() {
        Map<String,Object> mapSo = new HashMap<>();
        mapSo.put("k1", "one");
        jk.setReplicationInfo(mapSo);
        assertEquals("one",jk.getReplicationInfo().get("k1"));

    }

    @Test
    public void testGetDurabilityOfWrites() {
        jk.setDurabilityOfWrites("1");
        assertEquals("1",jk.getDurabilityOfWrites());
    }

    @Test
    public void testGetKeyspaceName() {
        jk.setKeyspaceName("Keyspace");
        assertEquals("Keyspace",jk.getKeyspaceName());
    }

}
