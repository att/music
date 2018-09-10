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
import java.util.ArrayList;
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.AAFResponse;
import org.onap.music.datastore.jsonobjects.NameSpace;

public class AAFResponseTest {

    @Test
    public void testGetNs() {
        NameSpace ns = new NameSpace();
        AAFResponse ar = new AAFResponse();
        ArrayList<NameSpace> nsArray = new ArrayList<>();
        ns.setName("tom");
        ArrayList<String> admin = new ArrayList<>();
        admin.add("admin1");
        ns.setAdmin(admin);
        nsArray.add(ns);
        ar.setNs(nsArray);
        assertEquals("tom",ar.getNs().get(0).getName());
        assertEquals("admin1",ar.getNs().get(0).getAdmin().get(0));
        
    }

//    @Test
//    public void testSetNs() {
//        fail("Not yet implemented");
//    }

}
