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
package org.onap.music.conductor.conditionals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.codehaus.jettison.json.JSONObject;
import org.onap.music.datastore.PreparedQueryObject;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.eelf.logging.format.AppMessages;
import org.onap.music.eelf.logging.format.ErrorSeverity;
import org.onap.music.eelf.logging.format.ErrorTypes;
import org.onap.music.main.MusicCore;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;
import org.onap.music.main.ReturnType;
import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.rest.RestMusicAdminAPI;
import org.onap.music.conductor.*;


import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TableMetadata;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiParam;

@Path("/v2/conditional")
@Api(value = "Conditional Api", hidden = true)
public class RestMusicConditionalAPI {
	private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicAdminAPI.class);
	private static final String XMINORVERSION = "X-minorVersion";
	private static final String XPATCHVERSION = "X-patchVersion";
	private static final String NS = "ns";
	private static final String VERSION = "v2";

	@POST
	@Path("/insert/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response insertConditional(
			@ApiParam(value = "Major Version", required = true) @PathParam("version") String version,
			@ApiParam(value = "Minor Version", required = false) @HeaderParam(XMINORVERSION) String minorVersion,
			@ApiParam(value = "Patch Version", required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @HeaderParam(NS) String ns,
			@ApiParam(value = "Authorization", required = true) @HeaderParam("Authorization") String authorization,
			@ApiParam(value = "Major Version", required = true) @PathParam("keyspace") String keyspace,
			@ApiParam(value = "Major Version", required = true) @PathParam("tablename") String tablename,
			JsonConditional jsonObj) throws Exception {
		ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);
		String primaryKey = jsonObj.getPrimaryKey();
		String primaryKeyValue = jsonObj.getPrimaryKeyValue();
		String casscadeColumnName = jsonObj.getCasscadeColumnName();
		Map<String, Object> tableValues = jsonObj.getTableValues();
		Map<String, Object> casscadeColumnData = jsonObj.getCasscadeColumnData();
		Map<String, Map<String, String>> conditions = jsonObj.getConditions();

		if (primaryKey == null || primaryKeyValue == null || casscadeColumnName == null || tableValues.isEmpty()
				|| casscadeColumnData.isEmpty() || conditions.isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
					ErrorTypes.AUTHENTICATIONERROR);
			return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE)
					.setError(String.valueOf("One or more input values missing")).toMap()).build();

		}
		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);

		Map<String, Object> authMap = null;
		try {
			authMap = MusicCore.authenticate(ns, userId, password, keyspace, aid, "insertIntoTable");
		} catch (Exception e) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
					ErrorTypes.AUTHENTICATIONERROR);
			return response.status(Status.UNAUTHORIZED)
					.entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
		}
		if (authMap.containsKey("aid"))
			authMap.remove("aid");
		if (!authMap.isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
					ErrorTypes.AUTHENTICATIONERROR);
			return response.status(Status.UNAUTHORIZED).entity(
					new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
					.build();
		}

		Map<String, Object> valuesMap = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : tableValues.entrySet()) {
			valuesMap.put(entry.getKey(), entry.getValue());
		}

		Map<String, String> status = new HashMap<>();
		status.put("exists", conditions.get("exists").get("status").toString());
		status.put("nonexists", conditions.get("nonexists").get("status").toString());
		ReturnType out = null;

		out = MusicConditional.conditionalInsert(keyspace, tablename, casscadeColumnName, casscadeColumnData,
				primaryKeyValue, valuesMap, status);
		return response.status(Status.OK).entity(new JsonResponse(out.getResult()).setMessage(out.getMessage()).toMap())
				.build();

	}

	@SuppressWarnings("unchecked")
	@PUT
	@Path("/update/keyspaces/{keyspace}/tables/{tablename}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateConditional(
			@ApiParam(value = "Major Version", required = true) @PathParam("version") String version,
			@ApiParam(value = "Minor Version", required = false) @HeaderParam(XMINORVERSION) String minorVersion,
			@ApiParam(value = "Patch Version", required = false) @HeaderParam(XPATCHVERSION) String patchVersion,
			@ApiParam(value = "AID", required = true) @HeaderParam("aid") String aid,
			@ApiParam(value = "Application namespace", required = true) @HeaderParam(NS) String ns,
			@ApiParam(value = "Authorization", required = true) @HeaderParam("Authorization") String authorization,
			@ApiParam(value = "Major Version", required = true) @PathParam("keyspace") String keyspace,
			@ApiParam(value = "Major Version", required = true) @PathParam("tablename") String tablename,
			JsonConditional upObj) throws Exception {
		ResponseBuilder response = MusicUtil.buildVersionResponse(VERSION, minorVersion, patchVersion);

		String primaryKey = upObj.getPrimaryKey();
		String primaryKeyValue = upObj.getPrimaryKeyValue();
		String casscadeColumnName = upObj.getCasscadeColumnName();
		Map<String, Object> casscadeColumnData = upObj.getCasscadeColumnData();
		Map<String, Object> tableValues = upObj.getTableValues();

		if (primaryKey == null || primaryKeyValue == null || casscadeColumnName == null
				|| casscadeColumnData.isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
					ErrorTypes.AUTHENTICATIONERROR);
			return response.status(Status.UNAUTHORIZED).entity(new JsonResponse(ResultType.FAILURE)
					.setError(String.valueOf("One or more input values missing")).toMap()).build();

		}
		Map<String,String> userCredentials = MusicUtil.extractBasicAuthentication(authorization);
		String userId = userCredentials.get(MusicUtil.USERID);
		String password = userCredentials.get(MusicUtil.PASSWORD);

		Map<String, Object> authMap = null;
		try {
			authMap = MusicCore.authenticate(ns, userId, password, keyspace, aid, "updateTable");
		} catch (Exception e) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
					ErrorTypes.AUTHENTICATIONERROR);
			return response.status(Status.UNAUTHORIZED)
					.entity(new JsonResponse(ResultType.FAILURE).setError(e.getMessage()).toMap()).build();
		}
		if (authMap.containsKey("aid"))
			authMap.remove("aid");
		if (!authMap.isEmpty()) {
			logger.error(EELFLoggerDelegate.errorLogger, "", AppMessages.MISSINGINFO, ErrorSeverity.CRITICAL,
					ErrorTypes.AUTHENTICATIONERROR);
			return response.status(Status.UNAUTHORIZED).entity(
					new JsonResponse(ResultType.FAILURE).setError(String.valueOf(authMap.get("Exception"))).toMap())
					.build();
		}

		String planId = casscadeColumnData.get("key").toString();
		Map<String,String> casscadeColumnValueMap = (Map<String, String>) casscadeColumnData.get("value");
		TableMetadata tableInfo = null;
		tableInfo = MusicCore.returnColumnMetadata(keyspace, tablename);
		DataType primaryIdType = tableInfo.getPrimaryKey().get(0).getType();
		String primaryId = tableInfo.getPrimaryKey().get(0).getName();
		
		PreparedQueryObject select = new PreparedQueryObject();
		select.appendQueryString("SELECT * FROM " + keyspace + "." + tablename + " where " + primaryId + " = ?");
		select.addValue(MusicUtil.convertToActualDataType(primaryIdType, primaryKeyValue));
		
		PreparedQueryObject upsert = MusicConditional.extractQuery(tableValues, tableInfo, tablename, keyspace, primaryKey, primaryKeyValue, null, null);
		Map<String,PreparedQueryObject> queryBank = new HashMap<>();
		queryBank.put(MusicUtil.SELECT, select);
		queryBank.put(MusicUtil.UPSERT, upsert);
		ReturnType result = MusicConditional.update(queryBank, keyspace, tablename, primaryKey,primaryKeyValue,planId,casscadeColumnName,casscadeColumnValueMap);
		if (result.getResult() == ResultType.SUCCESS) {
			return response.status(Status.OK)
					.entity(new JsonResponse(result.getResult()).setMessage(result.getMessage()).toMap()).build();

		}
		return response.status(Status.BAD_REQUEST)
				.entity(new JsonResponse(result.getResult()).setMessage(result.getMessage()).toMap()).build();

	}

}