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
import org.junit.Before;
import org.junit.Test;
import org.onap.music.datastore.jsonobjects.JsonOnboard;

public class JsonOnboardTest {

    JsonOnboard jo = null;
    
    @Before
    public void init() {
        jo = new JsonOnboard();
    }

    
    @Test
    public void testGetPassword() {
        String password = "password";
        jo.setPassword(password);
        assertEquals(password,jo.getPassword());
    }

    @Test
    public void testGetAid() {
        String aid = "aid";
        jo.setAid(aid);
        assertEquals(aid,jo.getAid());

    }

    @Test
    public void testGetAppname() {
        String appName = "appName";
        jo.setAppname(appName);
        assertEquals(appName,jo.getAppname());

    }

    @Test
    public void testGetUserId() {
        String userId = "userId";
        jo.setUserId(userId);
        assertEquals(userId,jo.getUserId());

    }

    @Test
    public void testGetIsAAF() {
        String aaf = "true";
        jo.setIsAAF(aaf);
        assertEquals(aaf,jo.getIsAAF());
        
    }

}
