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

import java.util.Map;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.onap.music.response.jsonobjects.JsonResponse;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.main.MusicUtil;
import org.onap.music.main.ResultType;

import com.att.eelf.configuration.EELFLogger;
import com.att.eelf.configuration.EELFManager;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;


@Path("/v{version: [0-9]+}/version")
@Api(value="Version Api")
public class RestMusicVersionAPI {

	private EELFLoggerDelegate logger =EELFLoggerDelegate.getLogger(RestMusicVersionAPI.class);

	/**
	 * Get the version of MUSIC
	 * @return
	 */
	@GET
	@ApiOperation(value = "Get Version", response = Map.class)
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String,Object> version(@Context HttpServletResponse response) {
		logger.info("Replying to request for MUSIC version with MUSIC:" + MusicUtil.getVersion());
		response.addHeader("X-latestVersion",MusicUtil.getVersion());
		return new JsonResponse(ResultType.SUCCESS).setMusicVersion("MUSIC:" + MusicUtil.getVersion()).toMap();
	}
}