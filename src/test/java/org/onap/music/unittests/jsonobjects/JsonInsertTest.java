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
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.JsonInsert;

public class JsonInsertTest {
    
    JsonInsert ji = new JsonInsert();

    @Test
    public void testGetKeyspaceName() {
        ji.setKeyspaceName("keyspace");
        assertEquals("keyspace",ji.getKeyspaceName());
    }

    @Test
    public void testGetTableName() {
        ji.setTableName("table");
        assertEquals("table",ji.getTableName());
    }

    @Test
    public void testGetConsistencyInfo() {
        Map<String,String> cons = new HashMap<>();
        cons.put("test","true");
        ji.setConsistencyInfo(cons);
        assertEquals("true",ji.getConsistencyInfo().get("test"));
    }

    @Test
    public void testGetTtl() {
        ji.setTtl("ttl");
        assertEquals("ttl",ji.getTtl());
    }

    @Test
    public void testGetTimestamp() {
        ji.setTimestamp("10:30");
        assertEquals("10:30",ji.getTimestamp());
    }

    @Test
    public void testGetValues() {
        Map<String,Object> cons = new HashMap<>();
        cons.put("val1","one");
        cons.put("val2","two");
        ji.setValues(cons);
        assertEquals("one",ji.getValues().get("val1"));
    }

    @Test
    public void testGetRow_specification() {
        Map<String,Object> cons = new HashMap<>();
        cons.put("val1","one");
        cons.put("val2","two");
        ji.setRow_specification(cons);
        assertEquals("two",ji.getRow_specification().get("val2"));
    }


}
