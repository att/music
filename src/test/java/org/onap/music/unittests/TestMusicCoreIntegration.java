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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.List;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.datastore.CassaLockStore;
import org.onap.music.datastore.MusicLockState;
import org.onap.music.datastore.MusicLockState.LockStatus;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestMusicCoreIntegration {

    static TestingServer zkServer;
    static PreparedQueryObject testObject;
    static String lockId = null;
    static String lockName = "ks1.tb1.pk1";

    @BeforeClass
    public static void init() throws Exception {
        try {
            MusicCore.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
            MusicCore.mLockHandle = new CassaLockStore(MusicCore.mDstoreHandle);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("####Port:" + zkServer.getPort());
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("After class");
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.dropKeyspace);
        MusicCore.eventualPut(testObject);
        MusicCore.deleteLock(lockName);
        MusicCore.mDstoreHandle.close();
        zkServer.stop();

    }

    @Test
    public void Test1_SetUp() throws MusicServiceException, MusicQueryException {
        MusicCore.mDstoreHandle = CassandraCQL.connectToEmbeddedCassandra();
        MusicCore.mLockHandle = new CassaLockStore(MusicCore.mDstoreHandle);
        ResultType result = ResultType.FAILURE;
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createKeySpace);
        MusicCore.eventualPut(testObject);
        testObject = new PreparedQueryObject();
        testObject.appendQueryString(CassandraCQL.createTableEmployees);
        result = MusicCore.nonKeyRelatedPut(testObject, MusicUtil.EVENTUAL);
        assertEquals(ResultType.SUCCESS, result);
    }

    @Test
    public void Test2_atomicPut() throws Exception {
        testObject = new PreparedQueryObject();
        testObject = CassandraCQL.setPreparedInsertQueryObject1();
        ReturnType returnType = MusicCore.atomicPut("testCassa", "employees", "Mr Test one",
                        testObject, null);
        assertEquals(ResultType.SUCCESS, returnType.getResult());
    }


    @Test
    public void Test5_atomicGet() throws Exception {
        testObject = new PreparedQueryObject();
        testObject = CassandraCQL.setPreparedGetQuery();
        ResultSet resultSet =
                        MusicCore.atomicGet("testCassa", "employees", "Mr Test two", testObject);
        List<Row> rows = resultSet.all();
        assertEquals(1, rows.size());
    }

    @Test
    public void Test6_createLockReference() throws Exception {
        lockId = MusicCore.createLockReference(lockName);
        assertNotNull(lockId);
    }

    @Test
    public void Test7_acquireLockwithLease() throws Exception {
        ReturnType lockLeaseStatus = MusicCore.acquireLockWithLease(lockName, lockId, 1000);
        assertEquals(ResultType.SUCCESS, lockLeaseStatus.getResult());
    }

    @Test
    public void Test8_acquireLock() throws Exception {
        ReturnType lockStatus = MusicCore.acquireLock(lockName, lockId);
        assertEquals(ResultType.SUCCESS, lockStatus.getResult());
    }

    @Test
    public void Test9_release() throws Exception {
        MusicLockState musicLockState = new MusicLockState(LockStatus.LOCKED, "id1");
        MusicLockState musicLockState1 = new MusicLockState(LockStatus.UNLOCKED, "id1");
        MusicCore.whoseTurnIsIt(lockName);
        MusicLockState mls = MusicCore.getMusicLockState(lockName);
        MusicLockState mls1 = MusicCore.voluntaryReleaseLock(lockName,lockId);
        assertEquals(musicLockState.getLockStatus(), mls.getLockStatus());
        assertEquals(musicLockState1.getLockStatus(), mls1.getLockStatus());
    }
}
