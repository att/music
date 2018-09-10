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

import java.util.HashMap;
import java.util.Map;

public class ReturnType {
    private ResultType result;
    private String message;

    public ReturnType(ResultType result, String message) {
        super();
        this.result = result;
        this.message = message;
    }

    public String getTimingInfo() {
        return timingInfo;
    }

    public void setTimingInfo(String timingInfo) {
        this.timingInfo = timingInfo;
    }

    private String timingInfo;

    public ResultType getResult() {
        return result;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String toJson() {
        return "{ \"result\":\"" + result.getResult() + "\", \"message\":\"" + message + "\"}";
    }

    public String toString() {
        return result + " | " + message;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> newMap = new HashMap<>();
        newMap.put("result", result.getResult());
        newMap.put("message", message);
        return newMap;
    }

}
