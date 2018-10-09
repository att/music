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
package org.onap.music.main;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;

import com.datastax.driver.core.DataType;
import com.sun.jersey.core.util.Base64;

/**
 * @author nelson24
 * 
 *         Properties This will take Properties and load them into MusicUtil.
 *         This is a hack for now. Eventually it would bebest to do this in
 *         another way.
 * 
 */
public class MusicUtil {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(MusicUtil.class);
    
    public static final String ATOMIC = "atomic";
    public static final String EVENTUAL = "eventual";
    public static final String CRITICAL = "critical";
    public static final String ATOMICDELETELOCK = "atomic_delete_lock";
    public static final String DEFAULTKEYSPACENAME = "TBD";
    private static final String XLATESTVERSION = "X-latestVersion";
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    public static final String SELECT = "select";
    public static final String INSERT = "insert";
    public static final String UPDATE = "update";
    public static final String UPSERT = "upsert";
    public static final String USERID = "userId";
    public static final String PASSWORD = "password";
    public static final String AUTHORIZATION = "Authorization";

    private static final String LOCALHOST = "localhost";
    private static final String PROPERTIES_FILE = "/opt/app/music/etc/music.properties";
    
    private static int myId = 0;
    private static ArrayList<String> allIds = new ArrayList<>();
    private static String publicIp = "";
    private static ArrayList<String> allPublicIps = new ArrayList<>();
    private static String myZkHost = LOCALHOST;
    private static String myCassaHost = LOCALHOST;
    private static String defaultMusicIp = LOCALHOST;
    private static boolean debug = true;
    private static String version = "2.3.0";
    private static String musicRestIp = LOCALHOST;
    private static String musicPropertiesFilePath = PROPERTIES_FILE;
    private static long defaultLockLeasePeriod = 6000;
    private static final String[] propKeys = new String[] { "zookeeper.host", "cassandra.host", "music.ip", "debug",
            "version", "music.rest.ip", "music.properties", "lock.lease.period", "id", "all.ids", "public.ip",
            "all.pubic.ips", "cassandra.user", "cassandra.password", "aaf.endpoint.url" };

    private static String cassName = "cassandra";
    private static String cassPwd;
    private static String aafEndpointUrl = null;
    public static ConcurrentMap<String, Long> zkNodeMap = new ConcurrentHashMap<>();
    
    public static final long MusicEternityEpochMillis = 1533081600000L; // Wednesday, August 1, 2018 12:00:00 AM
    
    public static final long MaxCriticalSectionDurationMillis = 1L * 24 * 60 * 60 * 1000; // 1 day

    private MusicUtil() {
        throw new IllegalStateException("Utility Class");
    }
    
    
    /**
     * @return the cassName
     */
    public static String getCassName() {
        return cassName;
    }

    /**
     * @return the cassPwd
     */
    public static String getCassPwd() {
        return cassPwd;
    }

    /**
     * @return the aafEndpointUrl
     */
    public static String getAafEndpointUrl() {
        return aafEndpointUrl;
    }

    /**
     * 
     * @param aafEndpointUrl
     */
    public static void setAafEndpointUrl(String aafEndpointUrl) {
        MusicUtil.aafEndpointUrl = aafEndpointUrl;
    }

    /**
     * 
     * @return
     */
    public static int getMyId() {
        return myId;
    }

    /**
     * 
     * @param myId
     */
    public static void setMyId(int myId) {
        MusicUtil.myId = myId;
    }

    /**
     * 
     * @return
     */
    public static List<String> getAllIds() {
        return allIds;
    }

    /**
     * 
     * @param allIds
     */
    public static void setAllIds(List<String> allIds) {
        MusicUtil.allIds = (ArrayList<String>) allIds;
    }

    /**
     * 
     * @return
     */
    public static String getPublicIp() {
        return publicIp;
    }

    /**
     * 
     * @param publicIp
     */
    public static void setPublicIp(String publicIp) {
        MusicUtil.publicIp = publicIp;
    }

    /**
     * 
     * @return
     */
    public static List<String> getAllPublicIps() {
        return allPublicIps;
    }

    /**
     * 
     * @param allPublicIps
     */
    public static void setAllPublicIps(List<String> allPublicIps) {
        MusicUtil.allPublicIps = (ArrayList<String>) allPublicIps;
    }

    /**
     * Returns An array of property names that should be in the Properties
     * files.
     * 
     * @return
     */
    public static String[] getPropkeys() {
        return propKeys;
    }

    /**
     * Get MusicRestIp - default = localhost property file value - music.rest.ip
     * 
     * @return
     */
    public static String getMusicRestIp() {
        return musicRestIp;
    }

    /**
     * Set MusicRestIp
     * 
     * @param musicRestIp
     */
    public static void setMusicRestIp(String musicRestIp) {
        MusicUtil.musicRestIp = musicRestIp;
    }

    /**
     * Get MusicPropertiesFilePath - Default = /opt/music/music.properties
     * property file value - music.properties
     * 
     * @return
     */
    public static String getMusicPropertiesFilePath() {
        return musicPropertiesFilePath;
    }

    /**
     * Set MusicPropertiesFilePath
     * 
     * @param musicPropertiesFilePath
     */
    public static void setMusicPropertiesFilePath(String musicPropertiesFilePath) {
        MusicUtil.musicPropertiesFilePath = musicPropertiesFilePath;
    }

    /**
     * Get DefaultLockLeasePeriod - Default = 6000 property file value -
     * lock.lease.period
     * 
     * @return
     */
    public static long getDefaultLockLeasePeriod() {
        return defaultLockLeasePeriod;
    }

    /**
     * Set DefaultLockLeasePeriod
     * 
     * @param defaultLockLeasePeriod
     */
    public static void setDefaultLockLeasePeriod(long defaultLockLeasePeriod) {
        MusicUtil.defaultLockLeasePeriod = defaultLockLeasePeriod;
    }

    /**
     * Set Debug
     * 
     * @param debug
     */
    public static void setDebug(boolean debug) {
        MusicUtil.debug = debug;
    }

    /**
     * Is Debug - Default = true property file value - debug
     * 
     * @return
     */
    public static boolean isDebug() {
        return debug;
    }

    /**
     * Set Version
     * 
     * @param version
     */
    public static void setVersion(String version) {
        MusicUtil.version = version;
    }

    /**
     * Return the version property file value - version
     * 
     * @return
     */
    public static String getVersion() {
        return version;
    }

    /**
     * Get MyZkHost - Zookeeper Hostname - Default = localhost property file
     * value - zookeeper.host
     * 
     * @return
     */
    public static String getMyZkHost() {
        return myZkHost;
    }

    /**
     * Set MyZkHost - Zookeeper Hostname
     * 
     * @param myZkHost
     */
    public static void setMyZkHost(String myZkHost) {
        MusicUtil.myZkHost = myZkHost;
    }

    /**
     * Get MyCassHost - Cassandra Hostname - Default = localhost property file
     * value - cassandra.host
     * 
     * @return
     */
    public static String getMyCassaHost() {
        return myCassaHost;
    }

    /**
     * Set MyCassHost - Cassandra Hostname
     * 
     * @param myCassaHost
     */
    public static void setMyCassaHost(String myCassaHost) {
        MusicUtil.myCassaHost = myCassaHost;
    }

    /**
     * Get DefaultMusicIp - Default = localhost property file value - music.ip
     * 
     * @return
     */
    public static String getDefaultMusicIp() {
        return defaultMusicIp;
    }

    /**
     * Set DefaultMusicIp
     * 
     * @param defaultMusicIp
     */
    public static void setDefaultMusicIp(String defaultMusicIp) {
        MusicUtil.defaultMusicIp = defaultMusicIp;
    }

    /**
     * 
     * @return
     */
    public static String getTestType() {
        String testType = "";
        try {
            Scanner fileScanner = new Scanner(new File(""));
            testType = fileScanner.next();// ignore the my id line
            @SuppressWarnings("unused")
			String batchSize = fileScanner.next();// ignore the my public ip
                                                    // line
            fileScanner.close();
        } catch (FileNotFoundException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
        }
        return testType;

    }

    /**
     * 
     * @param time
     */
    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            logger.error(EELFLoggerDelegate.errorLogger, e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Utility function to check if the query object is valid.
     * 
     * @param withparams
     * @param queryObject
     * @return
     */
    public static boolean isValidQueryObject(boolean withparams, PreparedQueryObject queryObject) {
        if (withparams) {
            int noOfValues = queryObject.getValues().size();
            int noOfParams = 0;
            char[] temp = queryObject.getQuery().toCharArray();
            for (int i = 0; i < temp.length; i++) {
                if (temp[i] == '?')
                    noOfParams++;
            }
            return (noOfValues == noOfParams);
        } else {
            return !queryObject.getQuery().isEmpty();
        }

    }

    public static void setCassName(String cassName) {
        MusicUtil.cassName = cassName;
    }

    public static void setCassPwd(String cassPwd) {
        MusicUtil.cassPwd = cassPwd;
    }

    @SuppressWarnings("unchecked")
	public static String convertToCQLDataType(DataType type, Object valueObj) throws Exception {

        String value = "";
        switch (type.getName()) {
        case UUID:
            value = valueObj + "";
            break;
        case TEXT:
        case VARCHAR:
            String valueString = valueObj + "";
            valueString = valueString.replace("'", "''");
            value = "'" + valueString + "'";
            break;
        case MAP: {
            Map<String, Object> otMap = (Map<String, Object>) valueObj;
            value = "{" + jsonMaptoSqlString(otMap, ",") + "}";
            break;
        }
        default:
            value = valueObj + "";
            break;
        }
        return value;
    }

    /**
     * 
     * @param colType
     * @param valueObj
     * @return
     * @throws MusicTypeConversionException 
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
	public static Object convertToActualDataType(DataType colType, Object valueObj) throws Exception {
        String valueObjString = valueObj + "";
        switch (colType.getName()) {
            case UUID:
                return UUID.fromString(valueObjString);
            case VARINT:
                return BigInteger.valueOf(Long.parseLong(valueObjString));
            case BIGINT:
                return Long.parseLong(valueObjString);
            case INT:
                return Integer.parseInt(valueObjString);
            case FLOAT:
                return Float.parseFloat(valueObjString);
            case DOUBLE:
                return Double.parseDouble(valueObjString);
            case BOOLEAN:
                return Boolean.parseBoolean(valueObjString);
            case MAP:
                return (Map<String, Object>) valueObj;
            case BLOB:
            	
            default:
                return valueObjString;
        }
    }

    public static ByteBuffer convertToActualDataType(DataType colType, byte[] valueObj) {
         ByteBuffer buffer = ByteBuffer.wrap(valueObj);
         return buffer;
    }
 
    /**
     *
     * Utility function to parse json map into sql like string
     * 
     * @param jMap
     * @param lineDelimiter
     * @return
     */

    public static String jsonMaptoSqlString(Map<String, Object> jMap, String lineDelimiter) throws Exception{
        StringBuilder sqlString = new StringBuilder();
        int counter = 0;
        for (Map.Entry<String, Object> entry : jMap.entrySet()) {
            Object ot = entry.getValue();
            String value = ot + "";
            if (ot instanceof String) {
                value = "'" + value.replace("'", "''") + "'";
            }
            sqlString.append("'" + entry.getKey() + "':" + value);
            if (counter != jMap.size() - 1)
                sqlString.append(lineDelimiter);
            counter = counter + 1;
        }
        return sqlString.toString();
    }

    @SuppressWarnings("unused")
    public static String buildVersion(String major, String minor, String patch) {
        if (minor != null) {
            major += "." + minor;
            if (patch != null) {
                major += "." + patch;
            }
        }
        return major;
    }
    
    /**
     * Currently this will build a header with X-latestVersion, X-minorVersion and X-pathcVersion
     * X-latestVerstion will be equal to the latest full version.
     * X-minorVersion - will be equal to the latest minor version.
     * X-pathVersion - will be equal to the latest patch version.
     * Future plans will change this. 
     * @param response
     * @param major
     * @param minor
     * @param patch
     * @return
     */
    public static ResponseBuilder buildVersionResponse(String major, String minor, String patch) {
        ResponseBuilder response = Response.noContent();
        String versionIn = buildVersion(major,minor,patch);
        String version = MusicUtil.getVersion();
        String[] verArray = version.split("\\.",3);
        if ( minor != null ) { 
            response.header(XMINORVERSION,minor);
        } else {
            response.header(XMINORVERSION,verArray[1]);
        } 
        if ( patch != null ) {
            response.header(XPATCHVERSION,patch);
        } else {
            response.header(XPATCHVERSION,verArray[2]);
        } 
        response.header(XLATESTVERSION,version);
        logger.info(EELFLoggerDelegate.applicationLogger,"Version In:" + versionIn);
        return response;
    }
    
    
    public static Map<String,String> extractBasicAuthentication(String authorization){
		
    	Map<String,String> authValues = new HashMap<>();
    	authorization = authorization.replaceFirst("Basic", "");
    	String decoded = Base64.base64Decode(authorization);
    	StringTokenizer token = new StringTokenizer(decoded, ":");
    	authValues.put(MusicUtil.USERID, token.nextToken().toString());
    	authValues.put(MusicUtil.PASSWORD,token.nextToken());
    	return authValues;
    	
    }

}
