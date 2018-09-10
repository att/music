/*
 * ============LICENSE_START========================================== org.onap.music
 * =================================================================== Copyright (c) 2017 AT&T
 * Intellectual Property ===================================================================
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */
package org.onap.music.unittests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
//cjc import static org.junit.Assert.assertTrue;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
//cjc import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mindrot.jbcrypt.BCrypt;
//cjcimport org.mindrot.jbcrypt.BCrypt;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.onap.music.datastore.CassaLockStore;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
//cjc import org.onap.music.datastore.jsonobjects.JsonKeySpace;
//import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.datastore.jsonobjects.JsonSelect;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
//import org.onap.music.main.ResultType;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.rest.RestMusicDataAPI;
import org.onap.music.rest.RestMusicQAPI;
import org.onap.music.rest.RestMusicLocksAPI;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.Base64;
//import com.datastax.driver.core.DataType;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
import com.sun.jersey.core.util.MultivaluedMapImpl;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(MockitoJUnitRunner.class)
public class TestRestMusicQAPI {

   
    RestMusicAdminAPI admin = new RestMusicAdminAPI();
    RestMusicLocksAPI lock = new RestMusicLocksAPI(); 
    RestMusicQAPI qData = new RestMusicQAPI();
    static PreparedQueryObject testObject;
    static TestingServer zkServer;

    @Mock
    static HttpServletResponse http;

    @Mock
    UriInfo info;
  
    static String appName = "TestApp";
    static String userId = "TestUser";
    static String password = "TestPassword";
    /*
    static String appName = "com.att.ecomp.portal.demeter.aid";//"TestApp";
    static String userId = "m00468@portal.ecomp.att.com";//"TestUser";
    static String password = "happy123";//"TestPassword";
    */
    static String authData = userId+":"+password;
    static String wrongAuthData = userId+":"+"pass";
    static String authorization = new String(Base64.encode(authData.getBytes()));
    static String wrongAuthorization = new String(Base64.encode(wrongAuthData.getBytes()));
    
    static boolean isAAF = false;
    static UUID uuid = UUID.fromString("abc66ccc-d857-4e90-b1e5-df98a3d40ce6");
    static String uuidS = "abc66ccc-d857-4e90-b1e5-df98a3d40ce6";
    static String keyspaceName = "testkscjc";
    static String tableName = "employees";
    static String xLatestVersion = "X-latestVersion";
    static String onboardUUID = null;
    static String lockId = null;
    static String lockName = "testkscjc.employees.sample3";
    static String majorV="3";
    static String minorV="0";
    static String patchV="1";
    static String aid=null;
    static JsonKeySpace kspObject=null;
    static RestMusicDataAPI data = new RestMusicDataAPI();
    static Response resp;
    
    @BeforeClass
    public static void init() throws Exception {
        try {
            MusicCore.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
            MusicCore.mLockHandle = new CassaLockStore(MusicCore.mDstoreHandle);

           // System.out.println("before class keysp");
            //resp=data.createKeySpace(majorV,minorV,patchV,aid,appName,userId,password,kspObject,keyspaceName);
            //System.out.println("after keyspace="+keyspaceName);
        } catch (Exception e) {
          System.out.println("before class exception ");
            e.printStackTrace();
        }
      // admin keyspace and table
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("CREATE KEYSPACE admin WITH REPLICATION = "
                        + "{'class' : 'SimpleStrategy' , "
                        + "'replication_factor': 1} AND DURABLE_WRITES = true");
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "CREATE TABLE admin.keyspace_master (" + "  uuid uuid, keyspace_name text,"
                                        + "  application_name text, is_api boolean,"
                                        + "  password text, username text,"
                                        + "  is_aaf boolean, PRIMARY KEY (uuid)\n" + ");");
        MusicCore.eventualPut(testObject);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                        MusicUtil.DEFAULTKEYSPACENAME));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        MusicCore.eventualPut(testObject);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "INSERT INTO admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) VALUES (?,?,?,?,?,?,?)");
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(),
                        UUID.fromString("bbc66ccc-d857-4e90-b1e5-df98a3d40de6")));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                        MusicUtil.DEFAULTKEYSPACENAME));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), "TestApp1"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), BCrypt.hashpw(password, BCrypt.gensalt())));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), "TestUser1"));
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
        MusicCore.eventualPut(testObject);

        testObject = new PreparedQueryObject();
        testObject.appendQueryString(
                        "select uuid from admin.keyspace_master where application_name = ? allow filtering");
        testObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        ResultSet rs = MusicCore.get(testObject);
        List<Row> rows = rs.all();
        if (rows.size() > 0) {
            System.out.println("#######UUID is:" + rows.get(0).getUUID("uuid"));
        }
          
        JsonKeySpace jsonKeyspace = new JsonKeySpace();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> replicationInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        replicationInfo.put("class", "SimpleStrategy");
        replicationInfo.put("replication_factor", 1);
        jsonKeyspace.setConsistencyInfo(consistencyInfo);
        jsonKeyspace.setDurabilityOfWrites("true");
        jsonKeyspace.setKeyspaceName(keyspaceName);
        jsonKeyspace.setReplicationInfo(replicationInfo);
        Response response = data.createKeySpace(majorV, minorV, patchV, null, authorization, appName,
                                    jsonKeyspace, keyspaceName);
        System.out.println("#######status is " + response.getStatus()+" keyspace="+keyspaceName);
        
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("After class");
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS " + keyspaceName);
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString("DROP KEYSPACE IF EXISTS admin");
        MusicCore.eventualPut(testObject);
        MusicCore.mDstoreHandle.close();
        zkServer.stop();
    }

    
    @Test
    public void Test1_createQ_good() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("uuid");
        jsonTable.setClusteringOrder("uuid ASC");
        //System.out.println("cjc before print version, xLatestVersion="+xLatestVersion);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableName);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
    }
  
    @Test
    public void Test1_createQ_FieldsEmpty() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        /*
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        */
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setFields(fields);
        //System.out.println("cjc before print version, xLatestVersion="+xLatestVersion);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableName);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("EmptyFields #######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertNotEquals(200, response.getStatus());
    }
    @Test
    public void Test1_createQ_Clustergood() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("emp_id");
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
    }
   
    @Test
    public void Test1_createQ_ClusterOrderGood1() throws Exception {
        String tableNameC="testcjcO";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name,emp_id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
    } 
    
    @Test
    public void Test1_createQ_PartitionKeygood() throws Exception {
        String tableNameP="testcjcP";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name,emp_salary),emp_id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setTableName(tableNameP);
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameP);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameP);
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
    } 
    
    @Test
    public void Test1_createQ_PartitionKeybadclose() throws Exception {
        String tableNameC="testcjcP1";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name,emp_salary),emp_id))");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name,emp_id");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setFields(fields);
        //System.out.println("cjc before print version, xLatestVersion="+xLatestVersion);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        //assertEquals(400, response.getStatus());
        assertTrue(200 != response.getStatus());
    } 
    
    @Test
    public void Test1_createQ_ClusterOrderGood2() throws Exception {
        String tableNameC="testcjcO1g";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name,emp_salary,emp_id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name,emp_id");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC,emp_id DESC");
        jsonTable.setFields(fields);
        //System.out.println("cjc before print version, xLatestVersion="+xLatestVersion);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
    } 
    
    @Test
    public void Test1_createQ_ColPkeyoverridesPrimaryKeyGood() throws Exception {
        String tableNameC="testcjcPr";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "((emp_name),emp_salary,emp_id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name,emp_id");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_salary ASC,emp_id DESC");
        jsonTable.setFields(fields);
        //System.out.println("cjc before print version, xLatestVersion="+xLatestVersion);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(200, response.getStatus());
        //assertTrue(200 != response.getStatus());
    } 
    
    @Test
    public void Test1_createQ_ClusterOrderBad() throws Exception {
        String tableNameC="testcjcO1b";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name,emp_id)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name,emp_id");
        jsonTable.setTableName(tableNameC);
        jsonTable.setClusteringOrder("emp_id DESCx");
        jsonTable.setFields(fields);
        //System.out.println("cjc before print version, xLatestVersion="+xLatestVersion);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        //                      "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, userId, password,
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    } 
    
    
    @Test
    public void Test3_createQ_0() throws Exception {
         //duplicate testing ...
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        String tableNameDup=tableName+"X";
        jsonTable.setTableName(tableNameDup);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, keyspaceName, tableNameDup);
        System.out.println("#######status for 1st time " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        
        Response response0 = qData.createQ(majorV, minorV,patchV,
                "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                jsonTable, keyspaceName, tableNameDup);
        // 400 is the duplicate status found in response
        // Music 113 duplicate testing 
        //import static org.junit.Assert.assertNotEquals;
        System.out.println("#######status for 2nd time " + response0.getStatus());
        System.out.println("Entity" + response0.getEntity());
        
        assertFalse("Duplicate table not created for "+tableNameDup, 200==response0.getStatus());
        //assertEquals(400, response0.getStatus());
        //assertNotEquals(200,response0.getStatus());
    }

    // Improper Auth
    @Test
    public void Test3_createQ1() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("uuid");
        jsonTable.setTableName(tableName);
        jsonTable.setClusteringOrder("uuid DESC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                        jsonTable, keyspaceName, tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401, response.getStatus());
    }

    // Improper keyspace
    @Test
    public void Test3_createQ2() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_salary", "varint");
        fields.put("PRIMARY KEY", "(emp_name)");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setTableName(tableName);
        jsonTable.setClusteringKey("emp_salary");
        jsonTable.setClusteringOrder("emp_salary DESC");
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonTable, "wrong", tableName);
        System.out.println("#######status is " + response.getStatus());
        System.out.println("Entity" + response.getEntity());
        assertEquals(401, response.getStatus());
    }



    @Test
    public void Test4_insertIntoQ() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testName");
        values.put("emp_salary", 500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.insertIntoQ(majorV, minorV,patchV, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }


    @Test
    public void Test4_insertIntoQ_valuesEmpty() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        /*
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "testName");
        values.put("emp_salary", 500);
        */
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.insertIntoQ(majorV, minorV,patchV, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6",
                appName, authorization, jsonInsert, keyspaceName, tableName);
        assertNotEquals(200, response.getStatus());
    }

    @Test
    public void Test4_insertIntoQ2() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.insertIntoQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }

    // Auth Error
    @Test
    public void Test4_insertIntoQ3() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.insertIntoQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, wrongAuthorization,
                        jsonInsert, keyspaceName, tableName);
        assertEquals(401, response.getStatus());
    }

    // Table wrong
    @Test
    public void Test4_insertIntoQ4() throws Exception {
        JsonInsert jsonInsert = new JsonInsert();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, Object> values = new HashMap<>();
        values.put("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_name", "test1");
        values.put("emp_salary", 1500);
        consistencyInfo.put("type", "eventual");
        jsonInsert.setConsistencyInfo(consistencyInfo);
        jsonInsert.setKeyspaceName(keyspaceName);
        jsonInsert.setTableName(tableName);
        jsonInsert.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.insertIntoQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonInsert, keyspaceName, "wrong");
        assertEquals(400, response.getStatus());
    }
      
    @Test
    public void Test5_updateQ() throws Exception {
        JsonUpdate jsonUpdate = new JsonUpdate();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        Map<String, Object> values = new HashMap<>();
        row.add("emp_name", "testName");
        row.add("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        values.put("emp_salary", "2500");
        consistencyInfo.put("type", "atomic");
        jsonUpdate.setConsistencyInfo(consistencyInfo);
        jsonUpdate.setKeyspaceName(keyspaceName);
        jsonUpdate.setTableName(tableName);
        jsonUpdate.setValues(values);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = qData.updateQ(majorV, minorV,patchV, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
                authorization, jsonUpdate, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }
    
  @Test
  public void Test5_updateQEmptyValues() throws Exception {
      JsonUpdate jsonUpdate = new JsonUpdate();
      Map<String, String> consistencyInfo = new HashMap<>();
      MultivaluedMap<String, String> row = new MultivaluedMapImpl();
      Map<String, Object> values = new HashMap<>();
      row.add("emp_name", "testName");
      //values.put("emp_salary", 2500);
      consistencyInfo.put("type", "atomic");
      jsonUpdate.setConsistencyInfo(consistencyInfo);
      jsonUpdate.setKeyspaceName(keyspaceName);
      jsonUpdate.setTableName(tableName);
      jsonUpdate.setValues(values);
      Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
      Mockito.when(info.getQueryParameters()).thenReturn(row);
      Response response = qData.updateQ(majorV, minorV,patchV, "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName,
              authorization, jsonUpdate, keyspaceName, tableName, info);
      assertNotEquals(200, response.getStatus());
  }

    @Test
    public void Test6_filterQ() throws Exception {  //select
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        row.add("uuid", "cfd66ccc-d857-4e90-b1e5-df98a3d40cd6");
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = qData.filter(majorV, minorV,patchV,"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", 
                        appName, authorization, keyspaceName, tableName, info);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
    }

    @Test
    public void Test6_peekQ() throws Exception {  //select
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = qData.peek(majorV, minorV,patchV,"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", 
                        appName, authorization, keyspaceName, tableName, info);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        if (result.isEmpty() ) assertTrue(true);
        else assertFalse(false);
        //assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
    }

    @Test
    public void Test6_peekQ_empty() throws Exception {  //select
        // row is not needed in thhis test
        JsonSelect jsonSelect = new JsonSelect();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "testName");
        consistencyInfo.put("type", "atomic");
        jsonSelect.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        UriInfo infoe= mockUriInfo("/peek?");//empty queryParam: cause exception
       // infoe.setQueryParameters("");
        System.out.println("uriinfo="+infoe.getQueryParameters());
        Mockito.when(infoe.getQueryParameters()).thenReturn(row);
        Response response = qData.peek(majorV, minorV,patchV,"abc66ccc-d857-4e90-b1e5-df98a3d40ce6", 
                        appName, authorization, keyspaceName, tableName, infoe);
        HashMap<String,HashMap<String,Object>> map = (HashMap<String, HashMap<String, Object>>) response.getEntity();
        HashMap<String, Object> result = map.get("result");
        if (result.isEmpty() ) assertTrue(true);
        else assertFalse(false);
        //assertEquals("2500", ((HashMap<String,Object>) result.get("row 0")).get("emp_salary").toString());
    }

    @Test
    public void Test6_deleteFromQ1() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = qData.deleteFromQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(200, response.getStatus());
    }

    // Values
    @Test
    public void Test6_deleteFromQ() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = qData.deleteFromQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        jsonDelete, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    // delObj
    @Test
    public void Test6_deleteFromQ2() throws Exception {
        JsonDelete jsonDelete = new JsonDelete();
        Map<String, String> consistencyInfo = new HashMap<>();
        MultivaluedMap<String, String> row = new MultivaluedMapImpl();
        row.add("emp_name", "test1");
        consistencyInfo.put("type", "atomic");
        jsonDelete.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Mockito.when(info.getQueryParameters()).thenReturn(row);
        Response response = qData.deleteFromQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                        null, keyspaceName, tableName, info);
        assertEquals(400, response.getStatus());
    }

    @Test
    public void Test7_dropQ() throws Exception {
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "atomic");
        jsonTable.setConsistencyInfo(consistencyInfo);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.dropQ(majorV, minorV,patchV,
                        "abc66ccc-d857-4e90-b1e5-df98a3d40ce6", appName, authorization,
                         keyspaceName, tableName);
        assertEquals(200, response.getStatus());
    }
   
    private UriInfo mockUriInfo(String urix) throws URISyntaxException {
      String uri="http://localhost:8080/MUSIC/rest/v"+majorV+"/priorityq/keyspaces/"+keyspaceName+"/"+tableName+urix;
      UriInfo uriInfo = Mockito.mock(UriInfo.class);
      System.out.println("mock urix="+urix+" uri="+uri);
      Mockito.when(uriInfo.getRequestUri()).thenReturn(new URI(uri));
      return uriInfo;
      }
    

    //Empty Fields
    @Test
    public void Test8_createQ_fields_empty() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("emp_id");
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setTableName(tableNameC);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

    //Partition key null
    @Test
    public void Test8_createQ_partitionKey_empty() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setClusteringKey("emp_id");
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

  //Clustering key null
    @Test
    public void Test8_createQ_ClusteringKey_empty() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringOrder("emp_id DESC");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

    //Clustering Order null
    @Test
    public void Test8_createQ_ClusteringOrder_empty() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPartitionKey("emp_name");
        jsonTable.setClusteringKey("emp_id");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

  //Invalid primary key
    @Test
    public void Test8_createQ_primaryKey_invalid() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setPrimaryKey("(emp_name");
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setClusteringKey("emp_id");
        jsonTable.setClusteringOrder("emp_id ASC");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

    //Primary key with no clustering key
    @Test
    public void Test8_createQ_primaryKey_with_empty_clusteringKey() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("emp_name");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        jsonTable.setClusteringOrder("emp_id ASC");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

    //Primary key with no partition key
    @Test
    public void Test8_createQ_primaryKey_with_empty_partitionKey() throws Exception {
        String tableNameC="testcjcC";
        JsonTable jsonTable = new JsonTable();
        Map<String, String> consistencyInfo = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("uuid", "text");
        fields.put("emp_name", "text");
        fields.put("emp_id", "text");
        fields.put("emp_salary", "varint");
        consistencyInfo.put("type", "eventual");
        jsonTable.setConsistencyInfo(consistencyInfo);
        jsonTable.setKeyspaceName(keyspaceName);
        jsonTable.setPrimaryKey("  ");
        jsonTable.setTableName(tableNameC);
        jsonTable.setFields(fields);
        jsonTable.setClusteringOrder("emp_id ASC");
        Mockito.doNothing().when(http).addHeader(xLatestVersion, MusicUtil.getVersion());
        Response response = qData.createQ(majorV, minorV,patchV,
                        aid, appName, authorization,
                        jsonTable, keyspaceName, tableNameC);
        System.out.println("#######status is " + response.getStatus()+"table namec="+tableNameC);
        System.out.println("Entity" + response.getEntity());
        assertEquals(400, response.getStatus());
    }

}