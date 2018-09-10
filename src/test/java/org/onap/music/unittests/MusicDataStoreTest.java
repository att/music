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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;

import org.onap.music.datastore.CassaDataStore;
import org.onap.music.datastore.PreparedQueryObject;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class MusicDataStoreTest {

    static CassaDataStore dataStore;
    static PreparedQueryObject testObject;

    @BeforeClass
    public static void init() {
        dataStore = CassandraCQL.connectToEmbeddedCassandra();

    }

    @AfterClass
    public static void close() throws MusicServiceException, MusicQueryException {
 
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.dropKeyspace);
        dataStore.executePut(testObject, "eventual");
        dataStore.close();

    }

    @Test
    public void Test1_SetUp() throws MusicServiceException, MusicQueryException {
        boolean result = false;
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createKeySpace);
        result = dataStore.executePut(testObject, "eventual");;
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createTableEmployees);
        result = dataStore.executePut(testObject, "eventual");
        assertEquals(true, result);

    }

    @Test
    public void Test2_ExecutePut_eventual_insert() throws MusicServiceException, MusicQueryException {
        testObject = CassandraCQL.setPreparedInsertQueryObject1();
        boolean result = dataStore.executePut(testObject, "eventual");
        assertEquals(true, result);
    }

    @Test
    public void Test3_ExecutePut_critical_insert() throws MusicServiceException, MusicQueryException {
        testObject = CassandraCQL.setPreparedInsertQueryObject2();
        boolean result = dataStore.executePut(testObject, "Critical");
        assertEquals(true, result);
    }

    @Test
    public void Test4_ExecutePut_eventual_update() throws MusicServiceException, MusicQueryException {
        testObject = CassandraCQL.setPreparedUpdateQueryObject();
        boolean result = false;
        result = dataStore.executePut(testObject, "eventual");
        assertEquals(true, result);
    }

    @Test
    public void Test5_ExecuteEventualGet() throws MusicServiceException, MusicQueryException {
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.selectALL);
        boolean result = false;
        int count = 0;
        ResultSet output = null;
        output = dataStore.executeEventualGet(testObject);
        System.out.println(output);
        ;
        for (Row row : output) {
            count++;
            System.out.println(row.toString());
        }
        if (count == 2) {
            result = true;
        }
        assertEquals(true, result);
    }

    @Test
    public void Test6_ExecuteCriticalGet() throws MusicServiceException, MusicQueryException {
        testObject = CassandraCQL.setPreparedGetQuery();
        boolean result = false;
        int count = 0;
        ResultSet output = null;
        output = dataStore.executeCriticalGet(testObject);
        System.out.println(output);
        ;
        for (Row row : output) {
            count++;
            System.out.println(row.toString());
        }
        if (count == 1) {
            result = true;
        }
        assertEquals(true, result);
    }

    @Test(expected = NullPointerException.class)
    public void Test7_exception() {
        PreparedQueryObject queryObject = null;
        try {
            dataStore.executePut(queryObject, "critical");
        } catch (MusicQueryException | MusicServiceException e) {
            System.out.println(e.getMessage());
        }
    }
    
    @Test
    public void Test8_columnDataType() {
        DataType data = dataStore.returnColumnDataType("testCassa", "employees", "empName");
        String datatype = data.toString();
        assertEquals("text",datatype);
    }
    
    @Test
    public void Test8_columnMetdaData() {
        TableMetadata data = dataStore.returnColumnMetadata("testCassa", "employees");
        assertNotNull(data);
    }
}
