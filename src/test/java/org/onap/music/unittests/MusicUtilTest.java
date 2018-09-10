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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.junit.Test;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.MusicUtil;
import com.datastax.driver.core.DataType;
import javassist.CodeConverter.ArrayAccessReplacementMethodNames;

public class MusicUtilTest {

    @Test
    public void testGetCassName() {
        MusicUtil.setCassName("Cassandra");
        assertTrue(MusicUtil.getCassName().equals("Cassandra"));
    }

    @Test
    public void testGetCassPwd() {
        MusicUtil.setCassPwd("Cassandra");
        assertTrue(MusicUtil.getCassPwd().equals("Cassandra"));
    }

    @Test
    public void testGetAafEndpointUrl() {
        MusicUtil.setAafEndpointUrl("url");
        assertEquals(MusicUtil.getAafEndpointUrl(),"url");
    }

    @Test
    public void testGetMyId() {
        MusicUtil.setMyId(1);
        assertEquals(MusicUtil.getMyId(),1);
    }

    @Test
    public void testGetAllIds() {
        List<String> ids = new ArrayList<String>();
        ids.add("1");
        ids.add("2");
        ids.add("3");
        MusicUtil.setAllIds(ids);
        assertEquals(MusicUtil.getAllIds().get(0),"1");
    }

//    @Test
//    public void testGetPublicIp() {
//        MusicUtil.setPublicIp("10.0.0.1");
//        assertEquals(MusicUtil.getPublicIp(),"10.0.0.1");
//    }

    @Test
    public void testGetAllPublicIps() {
        List<String> ips = new ArrayList<String>();
        ips.add("10.0.0.1");
        ips.add("10.0.0.2");
        ips.add("10.0.0.3");
        MusicUtil.setAllPublicIps(ips);
        assertEquals(MusicUtil.getAllPublicIps().get(1),"10.0.0.2");
    }

    @Test
    public void testGetPropkeys() {
        assertEquals(MusicUtil.getPropkeys()[2],"music.ip");
    }

    @Test
    public void testGetMusicRestIp() {
        MusicUtil.setMusicRestIp("localhost");
        assertEquals(MusicUtil.getMusicRestIp(),"localhost");
    }

    @Test
    public void testGetMusicPropertiesFilePath() {
        MusicUtil.setMusicPropertiesFilePath("filepath");
        assertEquals(MusicUtil.getMusicPropertiesFilePath(),"filepath");
    }

    @Test
    public void testGetDefaultLockLeasePeriod() {
        MusicUtil.setDefaultLockLeasePeriod(5000);
        assertEquals(MusicUtil.getDefaultLockLeasePeriod(),5000);
    }

    @Test
    public void testIsDebug() {
        MusicUtil.setDebug(true);
        assertTrue(MusicUtil.isDebug());
    }

    @Test
    public void testGetVersion() {
        MusicUtil.setVersion("1.0.0");
        assertEquals(MusicUtil.getVersion(),"1.0.0");
    }

    /*@Test
    public void testGetMyZkHost() {
        MusicUtil.setMyZkHost("10.0.0.2");
        assertEquals(MusicUtil.getMyZkHost(),"10.0.0.2");
    }*/

    @Test
    public void testGetMyCassaHost() {
        MusicUtil.setMyCassaHost("10.0.0.2");
        assertEquals(MusicUtil.getMyCassaHost(),"10.0.0.2");
    }

    @Test
    public void testGetDefaultMusicIp() {
        MusicUtil.setDefaultMusicIp("10.0.0.2");
        assertEquals(MusicUtil.getDefaultMusicIp(),"10.0.0.2");
    }

//    @Test
//    public void testGetTestType() {
//      fail("Not yet implemented"); // TODO
//    }

    @Test
    public void testIsValidQueryObject() {
        PreparedQueryObject myQueryObject = new PreparedQueryObject();
        myQueryObject.appendQueryString("select * from apple where type = ?");
        myQueryObject.addValue("macintosh");
        assertTrue(MusicUtil.isValidQueryObject(true,myQueryObject));

        myQueryObject.appendQueryString("select * from apple");
        assertTrue(MusicUtil.isValidQueryObject(false,myQueryObject));

        myQueryObject.appendQueryString("select * from apple where type = ?");
        assertFalse(MusicUtil.isValidQueryObject(true,myQueryObject));

        myQueryObject = new PreparedQueryObject();
        myQueryObject.appendQueryString("");
        System.out.println("#######" + myQueryObject.getQuery().isEmpty());
        assertFalse(MusicUtil.isValidQueryObject(false,myQueryObject));

    
    }

    @Test
    public void testConvertToCQLDataType() throws Exception {
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("name","tom");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.varchar(),"Happy People"),"'Happy People'");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.uuid(),UUID.fromString("29dc2afa-c2c0-47ae-afae-e72a645308ab")),"29dc2afa-c2c0-47ae-afae-e72a645308ab");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.blob(),"Hi"),"Hi");
        assertEquals(MusicUtil.convertToCQLDataType(DataType.map(DataType.varchar(),DataType.varchar()),myMap),"{'name':'tom'}");
    }

    @Test
    public void testConvertToActualDataType() throws Exception {
        assertEquals(MusicUtil.convertToActualDataType(DataType.varchar(),"Happy People"),"Happy People");
        assertEquals(MusicUtil.convertToActualDataType(DataType.uuid(),"29dc2afa-c2c0-47ae-afae-e72a645308ab"),UUID.fromString("29dc2afa-c2c0-47ae-afae-e72a645308ab"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.varint(),"1234"),BigInteger.valueOf(Long.parseLong("1234")));
        assertEquals(MusicUtil.convertToActualDataType(DataType.bigint(),"123"),Long.parseLong("123"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cint(),"123"),Integer.parseInt("123"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cfloat(),"123.01"),Float.parseFloat("123.01"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cdouble(),"123.02"),Double.parseDouble("123.02"));
        assertEquals(MusicUtil.convertToActualDataType(DataType.cboolean(),"true"),Boolean.parseBoolean("true"));
        Map<String,Object> myMap = new HashMap<String,Object>();
        myMap.put("name","tom");
        assertEquals(MusicUtil.convertToActualDataType(DataType.map(DataType.varchar(),DataType.varchar()),myMap),myMap);

    }

    @Test
    public void testJsonMaptoSqlString() throws Exception {
        Map<String,Object> myMap = new HashMap<>();
        myMap.put("name","tom");
        myMap.put("value",5);
        String result = MusicUtil.jsonMaptoSqlString(myMap,",");
        assertTrue(result.contains("name"));
        assertTrue(result.contains("value"));
    }

}
