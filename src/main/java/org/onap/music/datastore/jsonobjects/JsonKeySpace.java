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
package org.onap.music.datastore.jsonobjects;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

@ApiModel(value = "JsonTable", description = "Json model creating new keyspace")
@JsonIgnoreProperties(ignoreUnknown = true)
public class JsonKeySpace {
    private String keyspaceName;
    private Map<String, Object> replicationInfo;
    private String durabilityOfWrites;
    private Map<String, String> consistencyInfo;

    @ApiModelProperty(value = "Consistency level", allowableValues = "eventual,critical,atomic")
    public Map<String, String> getConsistencyInfo() {
        return consistencyInfo;
    }

    public void setConsistencyInfo(Map<String, String> consistencyInfo) {
        this.consistencyInfo = consistencyInfo;
    }

    @ApiModelProperty(value = "Replication information")
    public Map<String, Object> getReplicationInfo() {
        return replicationInfo;
    }

    public void setReplicationInfo(Map<String, Object> replicationInfo) {
        this.replicationInfo = replicationInfo;
    }

    @ApiModelProperty(value = "Durability", allowableValues = "true,false")
    public String getDurabilityOfWrites() {
        return durabilityOfWrites;
    }

    public void setDurabilityOfWrites(String durabilityOfWrites) {
        this.durabilityOfWrites = durabilityOfWrites;
    }

    @ApiModelProperty(value = "Keyspace name")
    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }



}
