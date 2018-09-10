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
package org.onap.music.rest;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;
import org.mindrot.jbcrypt.BCrypt;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.datastore.jsonobjects.JsonDelete;
import org.onap.music.datastore.jsonobjects.JsonInsert;
import org.onap.music.datastore.jsonobjects.JsonKeySpace;
import org.onap.music.datastore.jsonobjects.JsonTable;
import org.onap.music.datastore.jsonobjects.JsonUpdate;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.exceptions.MusicLockingException;
import org.onap.music.exceptions.MusicQueryException;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.exceptions.MusicServiceException;
import org.onap.music.main.CachingUtil;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicCore.Condition;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

/* Version 2 Class */
//@Path("/v{version: [0-9]+}/keyspaces")
@Path("/v2/keyspaces")
@Api(value = "Data Api")
public class RestMusicDataAPI {
    /*
     * Header values for Versioning X-minorVersion *** - Used to request or communicate a MINOR
     * version back from the client to the server, and from the server back to the client - This
     * will be the MINOR version requested by the client, or the MINOR version of the last MAJOR
     * version (if not specified by the client on the request) - Contains a single position value
     * (e.g. if the full version is 1.24.5, X-minorVersion = "24") - Is optional for the client on
     * request; however, this header should be provided if the client needs to take advantage of
     * MINOR incremented version functionality - Is mandatory for the server on response
     * 
     *** X-patchVersion *** - Used only to communicate a PATCH version in a response for
     * troubleshooting purposes only, and will not be provided by the client on request - This will
     * be the latest PATCH version of the MINOR requested by the client, or the latest PATCH version
     * of the MAJOR (if not specified by the client on the request) - Contains a single position
     * value (e.g. if the full version is 1.24.5, X-patchVersion = "5") - Is mandatory for the
     * server on response  (CURRENTLY NOT USED)
     *
     *** X-latestVersion *** - Used only to communicate an API's latest version - Is mandatory for the
     * server on response, and shall include the entire version of the API (e.g. if the full version
     * is 1.24.5, X-latestVersion = "1.24.5") - Used in the response to inform clients that they are
     * not using the latest version of the API (CURRENTLY NOT USED)
     *
     */

    private EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicDataAPI.class);
    private static final String XMINORVERSION = "X-minorVersion";
    private static final String XPATCHVERSION = "X-patchVersion";
    private static final String NS = "ns";
    private static final String VERSION = "v2";
    
    private class RowIdentifier {
        public String primarKeyValue;
        public StringBuilder rowIdString;
        @SuppressWarnings("unused")
        public PreparedQueryObject queryObject;// the string with all the row
                                               // identifiers separated by AND

        public RowIdentifier(String primaryKeyValue, StringBuilder rowIdString,
                        PreparedQueryObject queryObject) {
            this.primarKeyValue = primaryKeyValue;
            this.rowIdString = rowIdString;
            this.queryObject = queryObject;
        }
    }


    /**
     * Create Keyspace REST
     * 
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{name}")
    @ApiOperation(value = "Create Keyspace", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    //public Map<String, Object> createKeySpace(
    public Response createKeySpace(
                    @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
                    JsonKeySpace kspObject,
                    @ApiParam(value = "Keyspace Name",required = true) @PathParam("name") String keyspaceName) {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
        
		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = CachingUtil.verifyOnboarding(ns, userId, password);
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            response.status(Status.UNAUTHORIZED);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        if(kspObject == null || kspObject.getReplicationInfo() == null) {
            authMap.put(ResultType.EXCEPTION.getResult(), ResultType.BODYMISSING.getResult());
            response.status(Status.BAD_REQUEST);
            return response.entity(authMap).build();
        }


        try {
            authMap = MusicCore.authenticate(ns, userId, password, keyspaceName, aid,
                            "createKeySpace");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.MISSINGDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError("Unable to authenticate.").toMap()).build();
        }
        String newAid = null;
        if (!authMap.isEmpty()) {
            if (authMap.containsKey("aid")) {
                newAid = (String) authMap.get("aid");
            } else {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
                response.status(Status.UNAUTHORIZED);
                return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
            }
        }

        String consistency = MusicUtil.EVENTUAL;// for now this needs only
                                                // eventual consistency

        PreparedQueryObject queryObject = new PreparedQueryObject();
        long start = System.currentTimeMillis();
        Map<String, Object> replicationInfo = kspObject.getReplicationInfo();
        String repString = null;
        try {
            repString = "{" + MusicUtil.jsonMaptoSqlString(replicationInfo, ",") + "}";
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.MISSINGDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            
        }
        queryObject.appendQueryString(
                        "CREATE KEYSPACE " + keyspaceName + " WITH replication = " + repString);
        if (kspObject.getDurabilityOfWrites() != null) {
            queryObject.appendQueryString(
                            " AND durable_writes = " + kspObject.getDurabilityOfWrites());
        }

        queryObject.appendQueryString(";");
        long end = System.currentTimeMillis();
        logger.info(EELFLoggerDelegate.applicationLogger,
                        "Time taken for setting up query in create keyspace:" + (end - start));

        ResultType result = ResultType.FAILURE;
        try {
            result = MusicCore.nonKeyRelatedPut(queryObject, consistency);
            logger.info(EELFLoggerDelegate.applicationLogger, "result = " + result);
        } catch ( MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("err:" + ex.getMessage()).toMap()).build();
        }
        
        try {
            queryObject = new PreparedQueryObject();
            queryObject.appendQueryString("CREATE ROLE IF NOT EXISTS '" + userId
                            + "' WITH PASSWORD = '" + password + "' AND LOGIN = true;");
            MusicCore.nonKeyRelatedPut(queryObject, consistency);
            queryObject = new PreparedQueryObject();
            queryObject.appendQueryString("GRANT ALL PERMISSIONS on KEYSPACE " + keyspaceName
                                + " to '" + userId + "'");
            queryObject.appendQueryString(";");
            MusicCore.nonKeyRelatedPut(queryObject, consistency);
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
        }
        
        try {
            boolean isAAF = Boolean.valueOf(CachingUtil.isAAFApplication(ns));
            String hashedpwd = BCrypt.hashpw(password, BCrypt.gensalt());
            queryObject = new PreparedQueryObject();
            queryObject.appendQueryString(
                        "INSERT into admin.keyspace_master (uuid, keyspace_name, application_name, is_api, "
                                        + "password, username, is_aaf) values (?,?,?,?,?,?,?)");
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), newAid));
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), keyspaceName));
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), ns));
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), "True"));
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), hashedpwd));
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.text(), userId));
            queryObject.addValue(MusicUtil.convertToActualDataType(DataType.cboolean(), isAAF));
            CachingUtil.updateMusicCache(keyspaceName, ns);
            CachingUtil.updateMusicValidateCache(ns, userId, hashedpwd);
            MusicCore.eventualPut(queryObject);
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            return response.status(Response.Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        
        return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Created").toMap()).build();
    }

    /**
     * 
     * @param kspObject
     * @param keyspaceName
     * @return
     * @throws Exception
     */
    @DELETE
    @Path("/{name}")
    @ApiOperation(value = "Delete Keyspace", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    //public Map<String, Object> dropKeySpace(
    public Response dropKeySpace(
                    @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Keyspace Name",required = true) @PathParam("name") String keyspaceName) throws Exception {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = MusicCore.authenticate(ns, userId, password,keyspaceName, aid, "dropKeySpace");
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            return response.status(Status.UNAUTHORIZED).entity(authMap).build();
        }

        String consistency = MusicUtil.EVENTUAL;// for now this needs only
                                                // eventual
        // consistency
        String appName = CachingUtil.getAppName(keyspaceName);
        String uuid = CachingUtil.getUuidFromMusicCache(keyspaceName);
        PreparedQueryObject pQuery = new PreparedQueryObject();
        pQuery.appendQueryString(
                        "select  count(*) as count from admin.keyspace_master where application_name=? allow filtering;");
        pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(), appName));
        Row row = MusicCore.get(pQuery).one();
        long count = row.getLong(0);

        if (count == 0) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Keyspace not found. Please make sure keyspace exists.").toMap()).build();
        // Admin Functions:
        } else if (count == 1) {
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString(
                    "UPDATE admin.keyspace_master SET keyspace_name=? where uuid = ?;");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.text(),
                    MusicUtil.DEFAULTKEYSPACENAME));
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
            MusicCore.nonKeyRelatedPut(pQuery, consistency);
        } else {
            pQuery = new PreparedQueryObject();
            pQuery.appendQueryString("delete from admin.keyspace_master where uuid = ?");
            pQuery.addValue(MusicUtil.convertToActualDataType(DataType.uuid(), uuid));
            MusicCore.nonKeyRelatedPut(pQuery, consistency);
        }

        PreparedQueryObject queryObject = new PreparedQueryObject();
        queryObject.appendQueryString("DROP KEYSPACE " + keyspaceName + ";");
        ResultType result = MusicCore.nonKeyRelatedPut(queryObject, consistency);
        if ( result.equals(ResultType.FAILURE) ) {
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Error Deleteing Keyspace " + keyspaceName).toMap()).build();
        }
        return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("Keyspace " + keyspaceName + " Deleted").toMap()).build();
    }

    /**
     * 
     * @param tableObj
     * @param version
     * @param keyspace
     * @param tablename
     * @param headers
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{keyspace}/tables/{tablename}")
    @ApiOperation(value = "Create Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    //public Map<String, Object> createTable(
    public Response createTable(
                    @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                     JsonTable tableObj,
                    @ApiParam(value = "Keyspace Name",required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",required = true) @PathParam("tablename") String tablename) throws Exception {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = MusicCore.authenticate(ns, userId, password, keyspace,
                        aid, "createTable");
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        String consistency = MusicUtil.EVENTUAL;
        // for now this needs only eventual consistency

        String primaryKey = null;
        String partitionKey = tableObj.getPartitionKey();
        String clusterKey = tableObj.getClusteringKey();
        String filteringKey = tableObj.getFilteringKey();
        if(filteringKey != null) {
            clusterKey = clusterKey + "," + filteringKey;
        }
        primaryKey = tableObj.getPrimaryKey(); // get primaryKey if available

        PreparedQueryObject queryObject = new PreparedQueryObject();
        // first read the information about the table fields
        Map<String, String> fields = tableObj.getFields();
        StringBuilder fieldsString = new StringBuilder("(vector_ts text,");
        int counter = 0;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            if (entry.getKey().equals("PRIMARY KEY")) {
                primaryKey = entry.getValue(); // replaces primaryKey
                primaryKey.trim();
            } else {
                  if (counter == 0 )  fieldsString.append("" + entry.getKey() + " " + entry.getValue() + "");
                  else fieldsString.append("," + entry.getKey() + " " + entry.getValue() + "");             
            }

      	   if (counter != (fields.size() - 1) ) {
        	  
        	  //logger.info("cjc2 field="+entry.getValue()+"counter=" + counter+"fieldsize-1="+(fields.size() -1) + ",");
        	  counter = counter + 1; 
      	   } else {
         //logger.info("cjc3 field="+entry.getValue()+"counter=" + counter+"fieldsize="+fields.size() + ",");
               if((primaryKey != null) && (partitionKey == null)) {
                  primaryKey.trim();
                  int count1 = StringUtils.countMatches(primaryKey, ')');
                  int count2 = StringUtils.countMatches(primaryKey, '(');
                  if (count1 != count2) {
                        return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                             .setError("Create Table Error: primary key '(' and ')' do not match, primary key=" + primaryKey)
                                   .toMap()).build();
                  }

                if ( primaryKey.indexOf('(') == -1  || ( count2 == 1 && (primaryKey.lastIndexOf(")") +1) ==  primaryKey.length() ) )
                  {
               	      if (primaryKey.contains(",") ) {
                  	      partitionKey= primaryKey.substring(0,primaryKey.indexOf(","));
                   	      partitionKey=partitionKey.replaceAll("[\\(]+","");
                   	      clusterKey=primaryKey.substring(primaryKey.indexOf(',')+1);  // make sure index
                   	      clusterKey=clusterKey.replaceAll("[)]+", "");
               	      } else {
                    	  partitionKey=primaryKey;
                    	  partitionKey=partitionKey.replaceAll("[\\)]+","");
                   	      partitionKey=partitionKey.replaceAll("[\\(]+","");
                    	  clusterKey="";
                    }
                } else {   // not null and has ) before the last char
               	    partitionKey= primaryKey.substring(0,primaryKey.indexOf(')'));
               		partitionKey=partitionKey.replaceAll("[\\(]+","");
               		partitionKey.trim();
               		clusterKey= primaryKey.substring(primaryKey.indexOf(')'));
               		clusterKey=clusterKey.replaceAll("[\\(]+","");
               		clusterKey=clusterKey.replaceAll("[\\)]+","");
               		clusterKey.trim();
               		if (clusterKey.indexOf(",") == 0) clusterKey=clusterKey.substring(1);
               		   clusterKey.trim();
               		if (clusterKey.equals(",") ) clusterKey=""; // print error if needed    ( ... ),)

              } 

              if (!(partitionKey.isEmpty() || clusterKey.isEmpty())
                    && (partitionKey.equalsIgnoreCase(clusterKey) ||
                      clusterKey.contains(partitionKey) || partitionKey.contains(clusterKey)) )
               {
              	logger.error("DataAPI createTable partition/cluster key ERROR: partitionKey="+partitionKey+", clusterKey=" + clusterKey + " and primary key=" + primaryKey );
              		return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(
                            "Create Table primary key error: clusterKey(" + clusterKey + ") equals/contains/overlaps partitionKey(" +partitionKey+ ")  of"
                                    + " primary key=" + primaryKey)
                          	.toMap()).build();

        	}

            if (partitionKey.isEmpty() )  primaryKey="";
            else  if (clusterKey.isEmpty() ) primaryKey=" (" + partitionKey  + ")";
            else  primaryKey=" (" + partitionKey + ")," + clusterKey;

            //if (primaryKey != null) fieldsString.append("" + entry.getKey() + " (" + primaryKey + " )");
            if (primaryKey != null) fieldsString.append(", PRIMARY KEY (" + primaryKey + " )");

      } // end of length > 0
              else {
                 if (!(partitionKey.isEmpty() || clusterKey.isEmpty())
                        && (partitionKey.equalsIgnoreCase(clusterKey) ||
                          clusterKey.contains(partitionKey) || partitionKey.contains(clusterKey)) )
                   {
                     logger.error("DataAPI createTable partition/cluster key ERROR: partitionKey="+partitionKey+", clusterKey=" + clusterKey);
                     return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(
                                "Create Table primary key error: clusterKey(" + clusterKey + ") equals/contains/overlaps partitionKey(" +partitionKey+ ")")
                                .toMap()).build();
                }

                if (partitionKey.isEmpty() )  primaryKey="";
                else  if (clusterKey.isEmpty() ) primaryKey=" (" + partitionKey  + ")";
                else  primaryKey=" (" + partitionKey + ")," + clusterKey;

                //if (primaryKey != null) fieldsString.append("" + entry.getKey() + " (" + primaryKey + " )");
                if (primaryKey != null) fieldsString.append(", PRIMARY KEY (" + primaryKey + " )");
            }
      fieldsString.append(")");

     } // end of last field check

    } // end of for each
        // information about the name-value style properties
        Map<String, Object> propertiesMap = tableObj.getProperties();
        StringBuilder propertiesString = new StringBuilder();
        if (propertiesMap != null) {
            counter = 0;
            for (Map.Entry<String, Object> entry : propertiesMap.entrySet()) {
                Object ot = entry.getValue();
                String value = ot + "";
                if (ot instanceof String) {
                    value = "'" + value + "'";
                } else if (ot instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> otMap = (Map<String, Object>) ot;
                    value = "{" + MusicUtil.jsonMaptoSqlString(otMap, ",") + "}";
                }

                propertiesString.append(entry.getKey() + "=" + value + "");
                if (counter != propertiesMap.size() - 1)
                    propertiesString.append(" AND ");

                counter = counter + 1;
            }
        }

    String clusteringOrder = tableObj.getClusteringOrder();

    if (clusteringOrder != null && !(clusteringOrder.isEmpty())) {
       String[] arrayClusterOrder = clusteringOrder.split("[,]+");

       for (int i = 0; i < arrayClusterOrder.length; i++) 
	   {
           String[] clusterS = arrayClusterOrder[i].trim().split("[ ]+");
                if ( (clusterS.length ==2)  && (clusterS[1].equalsIgnoreCase("ASC") || clusterS[1].equalsIgnoreCase("DESC"))) continue;
                else {
                  //logger.error("createTable/Clustering Order vlaue ERROR: valid clustering order is ASC or DESC or expecting colname  order; please correct clusteringOrder:\"+ clusteringOrder+\".\"", " valid clustering order is ASC or DESC; please correct clusteringOrder:"+ clusteringOrder+".");
                    // logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGDATA,
                      //       ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                             return response.status(Status.BAD_REQUEST)
                                     .entity(new JsonResponse(ResultType.FAILURE)
                                             .setError("createTable/Clustering Order vlaue ERROR: valid clustering order is ASC or DESC or expecting colname  order; please correct clusteringOrder:"+ clusteringOrder+".")
                                                       .toMap()).build();
               }
        	// add validation for column names in cluster key
       }

       if (!(clusterKey.isEmpty())) 
       {
             	 clusteringOrder = "CLUSTERING ORDER BY (" +clusteringOrder +")";
             	 //cjc check if propertiesString.length() >0 instead propertiesMap
             	 if (propertiesMap != null)  propertiesString.append(" AND  "+ clusteringOrder);
                 else propertiesString.append(clusteringOrder);
       } else {
                logger.warn("Skipping clustering order=("+clusteringOrder+ ") since clustering key is empty ");
       }
    } //if non empty
	
	queryObject.appendQueryString(
	           "CREATE TABLE " + keyspace + "." + tablename + " " + fieldsString);


    if (propertiesString != null &&  propertiesString.length()>0 )
        queryObject.appendQueryString(" WITH " + propertiesString);
        queryObject.appendQueryString(";");
        ResultType result = ResultType.FAILURE;
        try {
          //logger.info("cjc query="+queryObject.getQuery());
            result = MusicCore.createTable(keyspace, tablename, queryObject, consistency);
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.MUSICSERVICEERROR);
            response.status(Status.BAD_REQUEST);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }
        if ( result.equals(ResultType.FAILURE) ) {
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(result).setError("Error Creating Table " + tablename).toMap()).build();
        }
        return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setMessage("TableName " + tablename + " Created under keyspace " + keyspace).toMap()).build();
    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param fieldName
     * @param info
     * @throws Exception
     */
    @POST
    @Path("/{keyspace}/tables/{tablename}/index/{field}")
    @ApiOperation(value = "Create Index", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createIndex(
                    @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    @ApiParam(value = "Keyspace Name",required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",required = true) @PathParam("tablename") String tablename,
                    @ApiParam(value = "Field Name",required = true) @PathParam("field") String fieldName,
                    @Context UriInfo info) throws Exception {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = MusicCore.authenticate(ns, userId, password, keyspace,aid, "createIndex");
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            response.status(Status.UNAUTHORIZED);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        MultivaluedMap<String, String> rowParams = info.getQueryParameters();
        String indexName = "";
        if (rowParams.getFirst("index_name") != null)
            indexName = rowParams.getFirst("index_name");
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("Create index " + indexName + " if not exists on " + keyspace + "."
                        + tablename + " (" + fieldName + ");");
        
        ResultType result = ResultType.FAILURE;
        try {
            result = MusicCore.nonKeyRelatedPut(query, "eventual");
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            response.status(Status.BAD_REQUEST);
            return response.entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }
        if ( result.equals(ResultType.SUCCESS) ) {
            return response.entity(new JsonResponse(result).setMessage("Index Created on " + keyspace+"."+tablename+"."+fieldName).toMap()).build();
        } else {
            return response.entity(new JsonResponse(result).setError("Unknown Error in create index.").toMap()).build();
        }
    }

    /**
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @return
     * @throws Exception
     */
    @POST
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Insert Into Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response insertIntoTable(
                    @ApiParam(value = "Major Version",required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    JsonInsert insObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename) {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        
        try {
            authMap = MusicCore.authenticate(ns, userId, password, keyspace,
                          aid, "insertIntoTable");
        } catch (Exception e) {
          logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
          return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.CRITICAL, ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }

        Map<String, Object> valuesMap = insObj.getValues();
        PreparedQueryObject queryObject = new PreparedQueryObject();
        TableMetadata tableInfo = null;
        try {
            tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
            if(tableInfo == null) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Table name doesn't exists. Please check the table name.").toMap()).build();
            }
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.CRITICAL, ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        String primaryKeyName = tableInfo.getPrimaryKey().get(0).getName();
        StringBuilder fieldsString = new StringBuilder("(vector_ts,");
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        StringBuilder valueString = new StringBuilder("(" + "?" + ",");
        queryObject.addValue(vectorTs);
        int counter = 0;
        String primaryKey = "";
        Map<String, byte[]> objectMap = insObj.getObjectMap();
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            fieldsString.append("" + entry.getKey());
            Object valueObj = entry.getValue();
            if (primaryKeyName.equals(entry.getKey())) {
                primaryKey = entry.getValue() + "";
                primaryKey = primaryKey.replace("'", "''");
            }
            DataType colType = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
            } catch(NullPointerException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage() +" Invalid column name : "+entry.getKey(), AppMessages.INCORRECTDATA  ,ErrorSeverity.CRITICAL, ErrorTypes.DATAERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Invalid column name : "+entry.getKey()).toMap()).build();
            }

            Object formattedValue = null;
            try {
              formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
          }
            valueString.append("?");
            
            queryObject.addValue(formattedValue);

            if (counter == valuesMap.size() - 1) {
                fieldsString.append(")");
                valueString.append(")");
            } else {
                fieldsString.append(",");
                valueString.append(",");
            }
            counter = counter + 1;
        }
        
        //blobs..
        if(objectMap != null) {
        for (Map.Entry<String, byte[]> entry : objectMap.entrySet()) {
        	if(counter > 0) {
        		fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ",");
        		valueString.replace(valueString.length()-1, valueString.length(), ",");
        	}
            fieldsString.append("" + entry.getKey());
            byte[] valueObj = entry.getValue();
            if (primaryKeyName.equals(entry.getKey())) {
                primaryKey = entry.getValue() + "";
                primaryKey = primaryKey.replace("'", "''");
            }

            DataType colType = tableInfo.getColumn(entry.getKey()).getType();

            ByteBuffer formattedValue = null;
            
            if(colType.toString().toLowerCase().contains("blob"))
            	formattedValue = MusicUtil.convertToActualDataType(colType, valueObj);
            
            valueString.append("?");
            
            queryObject.addValue(formattedValue);
            counter = counter + 1;
            /*if (counter == valuesMap.size() - 1) {
                fieldsString.append(")");
                valueString.append(")");
            } else {*/
                fieldsString.append(",");
                valueString.append(",");
            //}
        } }
        
        if(primaryKey == null || primaryKey.length() <= 0) {
            logger.error(EELFLoggerDelegate.errorLogger, "Some required partition key parts are missing: "+primaryKeyName );
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.SYNTAXERROR).setError("Some required partition key parts are missing: "+primaryKeyName).toMap()).build();
        }

        fieldsString.replace(fieldsString.length()-1, fieldsString.length(), ")");
		valueString.replace(valueString.length()-1, valueString.length(), ")");
		
        queryObject.appendQueryString("INSERT INTO " + keyspace + "." + tablename + " "
                        + fieldsString + " VALUES " + valueString);

        String ttl = insObj.getTtl();
        String timestamp = insObj.getTimestamp();

        if ((ttl != null) && (timestamp != null)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "both there");
            queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
            queryObject.addValue(Integer.parseInt(ttl));
            queryObject.addValue(Long.parseLong(timestamp));
        }

        if ((ttl != null) && (timestamp == null)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "ONLY TTL there");
            queryObject.appendQueryString(" USING TTL ?");
            queryObject.addValue(Integer.parseInt(ttl));
        }

        if ((ttl == null) && (timestamp != null)) {
            logger.info(EELFLoggerDelegate.applicationLogger, "ONLY timestamp there");
            queryObject.appendQueryString(" USING TIMESTAMP ?");
            queryObject.addValue(Long.parseLong(timestamp));
        }

        queryObject.appendQueryString(";");

        ReturnType result = null;
        String consistency = insObj.getConsistencyInfo().get("type");
        try {
            if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL)) {
                result = MusicCore.eventualPut(queryObject);
            } else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                String lockId = insObj.getConsistencyInfo().get("lockId");
                if(lockId == null) {
                    logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                }
                result = MusicCore.criticalPut(keyspace, tablename, primaryKey, queryObject, lockId,null);
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                result = MusicCore.atomicPut(keyspace, tablename, primaryKey, queryObject, null);

            }
        } catch (Exception ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }
        
        if (result==null) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.MUSICSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
        }
        return response.status(Status.OK).entity(new JsonResponse(result.getResult()).setMessage("Insert Successful").toMap()).build();
    }

    /**
     * 
     * @param insObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @PUT
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Update Table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response updateTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    JsonUpdate updateObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info) {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap;
        try {
            authMap = MusicCore.authenticate(ns, userId, password, keyspace,
                          aid, "updateTable");
        } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
              return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
              return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        long startTime = System.currentTimeMillis();
        String operationId = UUID.randomUUID().toString();// just for infoging
                                                          // purposes.
        String consistency = updateObj.getConsistencyInfo().get("type");
        logger.info(EELFLoggerDelegate.applicationLogger, "--------------Music " + consistency
                        + " update-" + operationId + "-------------------------");
        // obtain the field value pairs of the update

        PreparedQueryObject queryObject = new PreparedQueryObject();
        Map<String, Object> valuesMap = updateObj.getValues();

        TableMetadata tableInfo;
        try {
            tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
        } catch (MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (tableInfo == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                            .setError("Table information not found. Please check input for table name= "
                                            + keyspace + "." + tablename).toMap()).build();
        }
        String vectorTs =
                        String.valueOf(Thread.currentThread().getId() + System.currentTimeMillis());
        StringBuilder fieldValueString = new StringBuilder("vector_ts=?,");
        queryObject.addValue(vectorTs);
        int counter = 0;
        for (Map.Entry<String, Object> entry : valuesMap.entrySet()) {
            Object valueObj = entry.getValue();
            DataType colType = null;
            try {
                colType = tableInfo.getColumn(entry.getKey()).getType();
            } catch(NullPointerException ex) {
                logger.error(EELFLoggerDelegate.errorLogger, "Invalid column name : "+entry.getKey());
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Invalid column name : "+entry.getKey()).toMap()).build();
            }
            Object valueString = null;
            try {
              valueString = MusicUtil.convertToActualDataType(colType, valueObj);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            }
            fieldValueString.append(entry.getKey() + "= ?");
            queryObject.addValue(valueString);
            if (counter != valuesMap.size() - 1)
                fieldValueString.append(",");
            counter = counter + 1;
        }
        String ttl = updateObj.getTtl();
        String timestamp = updateObj.getTimestamp();

        queryObject.appendQueryString("UPDATE " + keyspace + "." + tablename + " ");
        if ((ttl != null) && (timestamp != null)) {

            logger.info("both there");
            queryObject.appendQueryString(" USING TTL ? AND TIMESTAMP ?");
            queryObject.addValue(Integer.parseInt(ttl));
            queryObject.addValue(Long.parseLong(timestamp));
        }

        if ((ttl != null) && (timestamp == null)) {
            logger.info("ONLY TTL there");
            queryObject.appendQueryString(" USING TTL ?");
            queryObject.addValue(Integer.parseInt(ttl));
        }

        if ((ttl == null) && (timestamp != null)) {
            logger.info("ONLY timestamp there");
            queryObject.appendQueryString(" USING TIMESTAMP ?");
            queryObject.addValue(Long.parseLong(timestamp));
        }
        // get the row specifier
        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
            if(rowId == null || rowId.primarKeyValue.isEmpty()) {
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                        .setError("Mandatory WHERE clause is missing. Please check the input request.").toMap()).build();
            }
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }

        queryObject.appendQueryString(
                        " SET " + fieldValueString + " WHERE " + rowId.rowIdString + ";");

        // get the conditional, if any
        Condition conditionInfo;
        if (updateObj.getConditions() == null)
            conditionInfo = null;
        else {// to avoid parsing repeatedly, just send the select query to
              // obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                            + rowId.rowIdString + ";");
            selectQuery.addValue(rowId.primarKeyValue);
            conditionInfo = new MusicCore.Condition(updateObj.getConditions(), selectQuery);
        }

        ReturnType operationResult = null;
        long jsonParseCompletionTime = System.currentTimeMillis();

        if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
            operationResult = MusicCore.eventualPut(queryObject);
        else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            String lockId = updateObj.getConsistencyInfo().get("lockId");
            if(lockId == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                        + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                        + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
            }
            operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                            queryObject, lockId, conditionInfo);
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            try {
              operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                              queryObject, conditionInfo);
            } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {
                logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
            }
        }
        long actualUpdateCompletionTime = System.currentTimeMillis();

        long endTime = System.currentTimeMillis();
        String timingString = "Time taken in ms for Music " + consistency + " update-" + operationId
                        + ":" + "|total operation time:" + (endTime - startTime)
                        + "|json parsing time:" + (jsonParseCompletionTime - startTime)
                        + "|update time:" + (actualUpdateCompletionTime - jsonParseCompletionTime)
                        + "|";

        if (operationResult != null && operationResult.getTimingInfo() != null) {
            String lockManagementTime = operationResult.getTimingInfo();
            timingString = timingString + lockManagementTime;
        }
        logger.info(EELFLoggerDelegate.applicationLogger, timingString);
        
        if (operationResult==null) {
            logger.error(EELFLoggerDelegate.errorLogger,"Null result - Please Contact admin", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
        }
        if ( operationResult.getResult() == ResultType.SUCCESS ) {
            return response.status(Status.OK).entity(new JsonResponse(operationResult.getResult()).setMessage(operationResult.getMessage()).toMap()).build();
        } else {
            logger.error(EELFLoggerDelegate.errorLogger,operationResult.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(operationResult.getResult()).setError(operationResult.getMessage()).toMap()).build();
        }
        
    }

    /**
     * 
     * @param delObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Delete From table", response = String.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteFromTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    JsonDelete delObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info) {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = null;
        try {
            authMap = MusicCore.authenticate(ns, userId, password, keyspace,
                            aid, "deleteFromTable");
        } catch (Exception e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
        }
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
              return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        if(delObj == null) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGDATA  ,ErrorSeverity.WARN, ErrorTypes.DATAERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Required HTTP Request body is missing.").toMap()).build();
        }
        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder columnString = new StringBuilder();

        int counter = 0;
        ArrayList<String> columnList = delObj.getColumns();
        if (columnList != null) {
            for (String column : columnList) {
                columnString.append(column);
                if (counter != columnList.size() - 1)
                    columnString.append(",");
                counter = counter + 1;
            }
        }

        // get the row specifier
        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }
        String rowSpec = rowId.rowIdString.toString();

        if ((columnList != null) && (!rowSpec.isEmpty())) {
            queryObject.appendQueryString("DELETE " + columnString + " FROM " + keyspace + "."
                            + tablename + " WHERE " + rowSpec + ";");
        }

        if ((columnList == null) && (!rowSpec.isEmpty())) {
            queryObject.appendQueryString("DELETE FROM " + keyspace + "." + tablename + " WHERE "
                            + rowSpec + ";");
        }

        if ((columnList != null) && (rowSpec.isEmpty())) {
            queryObject.appendQueryString(
                            "DELETE " + columnString + " FROM " + keyspace + "." + rowSpec + ";");
        }
        // get the conditional, if any
        Condition conditionInfo;
        if (delObj.getConditions() == null)
            conditionInfo = null;
        else {// to avoid parsing repeatedly, just send the select query to
              // obtain row
            PreparedQueryObject selectQuery = new PreparedQueryObject();
            selectQuery.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + " WHERE "
                            + rowId.rowIdString + ";");
            selectQuery.addValue(rowId.primarKeyValue);
            conditionInfo = new MusicCore.Condition(delObj.getConditions(), selectQuery);
        }

        String consistency = delObj.getConsistencyInfo().get("type");

        ReturnType operationResult = null;
        try {
            if (consistency.equalsIgnoreCase(MusicUtil.EVENTUAL))
                operationResult = MusicCore.eventualPut(queryObject);
            else if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
                String lockId = delObj.getConsistencyInfo().get("lockId");
                if(lockId == null) {
                    logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                            + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                    return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                            + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
                }
                operationResult = MusicCore.criticalPut(keyspace, tablename, rowId.primarKeyValue,
                                queryObject, lockId, conditionInfo);
            } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
                    operationResult = MusicCore.atomicPut(keyspace, tablename, rowId.primarKeyValue,
                                    queryObject, conditionInfo);
            }
        } catch (MusicLockingException | MusicQueryException | MusicServiceException e) {
            logger.error(EELFLoggerDelegate.errorLogger,e.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE)
                    .setError("Unable to perform Delete operation. Exception from music").toMap()).build();
        }
        if (operationResult==null) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("Null result - Please Contact admin").toMap()).build();
        }
        if (operationResult.getResult().equals(ResultType.SUCCESS)) {
            return response.status(Status.OK).entity(new JsonResponse(operationResult.getResult()).setMessage(operationResult.getMessage()).toMap()).build();
        } else {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(operationResult.getMessage()).toMap()).build();
        }
    }

    /**
     * 
     * @param tabObj
     * @param keyspace
     * @param tablename
     * @throws Exception
     */
    @DELETE
    @Path("/{keyspace}/tables/{tablename}")
    @ApiOperation(value = "Drop Table", response = String.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response dropTable(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename) throws Exception {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap =
                        MusicCore.authenticate(ns, userId, password, keyspace, aid, "dropTable");
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        String consistency = "eventual";// for now this needs only eventual
                                        // consistency
        PreparedQueryObject query = new PreparedQueryObject();
        query.appendQueryString("DROP TABLE  " + keyspace + "." + tablename + ";");
        try {
            return response.status(Status.OK).entity(new JsonResponse(MusicCore.nonKeyRelatedPut(query, consistency)).toMap()).build();
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }

    }

    /**
     * 
     * @param selObj
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     */
    @PUT
    @Path("/{keyspace}/tables/{tablename}/rows/criticalget")
    @ApiOperation(value = "Select Critical", response = Map.class)
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response selectCritical(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    JsonInsert selObj,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info) throws Exception {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap = MusicCore.authenticate(ns, userId, password, keyspace,aid, "selectCritical");
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"Error while authentication... ", AppMessages.MISSINGINFO  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
              return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        String lockId = selObj.getConsistencyInfo().get("lockId");

        PreparedQueryObject queryObject = new PreparedQueryObject();

        RowIdentifier rowId = null;
        try {
            rowId = getRowIdentifier(keyspace, tablename, info.getQueryParameters(), queryObject);
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,ex.getMessage(), AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
              return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }
        queryObject.appendQueryString(
                        "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowId.rowIdString + ";");

        ResultSet results = null;

        String consistency = selObj.getConsistencyInfo().get("type");

        if (consistency.equalsIgnoreCase(MusicUtil.CRITICAL)) {
            if(lockId == null) {
                logger.error(EELFLoggerDelegate.errorLogger,"LockId cannot be null. Create lock reference or"
                        + " use ATOMIC instead of CRITICAL", ErrorSeverity.FATAL, ErrorTypes.MUSICSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError("LockId cannot be null. Create lock "
                        + "and acquire lock or use ATOMIC instead of CRITICAL").toMap()).build();
            }
            results = MusicCore.criticalGet(keyspace, tablename, rowId.primarKeyValue, queryObject,
                            lockId);
        } else if (consistency.equalsIgnoreCase(MusicUtil.ATOMIC)) {
            results = MusicCore.atomicGet(keyspace, tablename, rowId.primarKeyValue, queryObject);
        }
        if(results!=null && results.getAvailableWithoutFetching() >0) {
        	return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicCore.marshallResults(results)).toMap()).build();
        }
        return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setError("No data found").toMap()).build();

        
    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param info
     * @return
     * @throws Exception
     */
    @GET
    @Path("/{keyspace}/tables/{tablename}/rows")
    @ApiOperation(value = "Select All or Select Specific", response = Map.class)
    @Produces(MediaType.APPLICATION_JSON)
    public Response select(
                    @ApiParam(value = "Major Version",
                                    required = true) @PathParam("version") String version,
                    @ApiParam(value = "Minor Version",
                                    required = false) @HeaderParam(XMINORVERSION) String minorVersion,
                    @ApiParam(value = "Patch Version",
                                    required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
                    @ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
                    @ApiParam(value = "Application namespace",
                                    required = true) @HeaderParam(NS) String ns,
                    @ApiParam(value = "Authorization", required = true) @HeaderParam(MusicUtil.AUTHORIZATION) String authorization,
                    @ApiParam(value = "Keyspace Name",
                                    required = true) @PathParam("keyspace") String keyspace,
                    @ApiParam(value = "Table Name",
                                    required = true) @PathParam("tablename") String tablename,
                    @Context UriInfo info) throws Exception {
        ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);
        Map<String, Object> authMap =
                        MusicCore.authenticate(ns, userId, password, keyspace, aid, "select");
        if (authMap.containsKey("aid"))
            authMap.remove("aid");
        if (!authMap.isEmpty()) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.AUTHENTICATIONERROR  ,ErrorSeverity.WARN, ErrorTypes.AUTHENTICATIONERROR);
            return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap()).build();
        }
        PreparedQueryObject queryObject = new PreparedQueryObject();

        if (info.getQueryParameters().isEmpty())// select all
            queryObject.appendQueryString("SELECT *  FROM " + keyspace + "." + tablename + ";");
        else {
            int limit = -1; // do not limit the number of results
            try {
                queryObject = selectSpecificQuery(VERSION, minorVersion, patchVersion, aid, ns,
                                userId, password, keyspace, tablename, info, limit);
            } catch (MusicServiceException ex) {
                logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.WARN, ErrorTypes.GENERALSERVICEERROR);
                return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
            }
        }

        try {
            ResultSet results = MusicCore.get(queryObject);
            if(results.getAvailableWithoutFetching() >0) {
            	return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setDataResult(MusicCore.marshallResults(results)).toMap()).build();
            }
            return response.status(Status.OK).entity(new JsonResponse(ResultType.SUCCESS).setError("No data found").toMap()).build();
        } catch (MusicServiceException ex) {
            logger.error(EELFLoggerDelegate.errorLogger,"", AppMessages.UNKNOWNERROR  ,ErrorSeverity.ERROR, ErrorTypes.MUSICSERVICEERROR);
            return response.status(Status.BAD_REQUEST).entity(new JsonResponse(ResultType.FAILURE).setError(ex.getMessage()).toMap()).build();
        }

    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param info
     * @param limit
     * @return
     * @throws MusicServiceException
     */
    public PreparedQueryObject selectSpecificQuery(String version, String minorVersion,
                    String patchVersion, String aid, String ns, String userId, String password,
                    String keyspace, String tablename, UriInfo info, int limit)
                    throws MusicServiceException {

        PreparedQueryObject queryObject = new PreparedQueryObject();
        StringBuilder rowIdString = getRowIdentifier(keyspace, tablename, info.getQueryParameters(),
                        queryObject).rowIdString;

        queryObject.appendQueryString(
                        "SELECT *  FROM " + keyspace + "." + tablename + " WHERE " + rowIdString);

        if (limit != -1) {
            queryObject.appendQueryString(" LIMIT " + limit);
        }

        queryObject.appendQueryString(";");
        return queryObject;

    }

    /**
     * 
     * @param keyspace
     * @param tablename
     * @param rowParams
     * @param queryObject
     * @return
     * @throws MusicServiceException
     */
    private RowIdentifier getRowIdentifier(String keyspace, String tablename,
                    MultivaluedMap<String, String> rowParams, PreparedQueryObject queryObject)
                    throws MusicServiceException {
        StringBuilder rowSpec = new StringBuilder();
        int counter = 0;
        TableMetadata tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
        if (tableInfo == null) {
            logger.error(EELFLoggerDelegate.errorLogger,
                            "Table information not found. Please check input for table name= "
                                            + keyspace + "." + tablename);
            throw new MusicServiceException(
                            "Table information not found. Please check input for table name= "
                                            + keyspace + "." + tablename);
        }
        StringBuilder primaryKey = new StringBuilder();
        for (MultivaluedMap.Entry<String, List<String>> entry : rowParams.entrySet()) {
            String keyName = entry.getKey();
            List<String> valueList = entry.getValue();
            String indValue = valueList.get(0);
            DataType colType = null;
            Object formattedValue = null;
            try {
              colType = tableInfo.getColumn(entry.getKey()).getType();
              formattedValue = MusicUtil.convertToActualDataType(colType, indValue);
            } catch (Exception e) {
              logger.error(EELFLoggerDelegate.errorLogger,e.getMessage());
            }
            if(tableInfo.getPrimaryKey().get(0).getName().equals(entry.getKey()))
            primaryKey.append(indValue);
            rowSpec.append(keyName + "= ?");
            queryObject.addValue(formattedValue);
            if (counter != rowParams.size() - 1)
                rowSpec.append(" AND ");
            counter = counter + 1;
        }
        return new RowIdentifier(primaryKey.toString(), rowSpec, queryObject);
    }
}
