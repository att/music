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
import org.junit.Test;
import org.onap.music.main.ResultType;

public class ResultTypeTest {

    @Test
    public void testResultType() {
        assertEquals("SUCCESS",ResultType.SUCCESS.name());
        assertEquals("FAILURE",ResultType.FAILURE.name());
    }

    @Test
    public void testGetResult() {
        assertEquals("Success",ResultType.SUCCESS.getResult());
        assertEquals("Failure",ResultType.FAILURE.getResult());
    }

}
