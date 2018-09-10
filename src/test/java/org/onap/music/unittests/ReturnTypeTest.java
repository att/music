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

package org.onap.music.unittests;

import static org.junit.Assert.*;
import java.util.Map;
import org.apache.tools.ant.filters.TokenFilter.ContainsString;
import org.hamcrest.core.AnyOf;
import org.junit.Test;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;

public class ReturnTypeTest {

    @Test
    public void testReturnType() {
        ReturnType result = new ReturnType(ResultType.SUCCESS,"message");
        assertEquals(result.getMessage(),"message");
        assertEquals(result.getResult(),ResultType.SUCCESS);
    }

    @Test
    public void testTimingInfo() {
        ReturnType result = new ReturnType(ResultType.SUCCESS,"message");
        result.setTimingInfo("123");
        assertEquals(result.getTimingInfo(),"123");
    }

    @Test
    public void testGetResult() {
        ReturnType result = new ReturnType(ResultType.FAILURE,"message");
        assertEquals(result.getResult(),ResultType.FAILURE);
    }

    @Test
    public void testGetMessage() {
        ReturnType result = new ReturnType(ResultType.SUCCESS,"message");
        result.setMessage("NewMessage");
        assertEquals(result.getMessage(),"NewMessage");
    }

    @Test
    public void testToJson() {
        ReturnType result = new ReturnType(ResultType.SUCCESS,"message");
        String myJson = result.toJson();
        assertTrue(myJson.contains("message"));
    }

    @Test
    public void testToString() {
        ReturnType result = new ReturnType(ResultType.SUCCESS,"message");
        String test = result.toString();
        assertTrue(test.contains("message"));
    }

    @Test
    public void testToMap() {
        ReturnType result = new ReturnType(ResultType.SUCCESS,"message");
        Map<String, Object> myMap = result.toMap();
        assertTrue(myMap.containsKey("message"));
    }

}
