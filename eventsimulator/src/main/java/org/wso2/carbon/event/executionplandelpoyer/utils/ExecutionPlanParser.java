/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.event.executionplandelpoyer.utils;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDto;
import org.wso2.carbon.event.executionplandelpoyer.Queries;
import org.wso2.carbon.event.executionplandelpoyer.StreamDefinitionDto;

import java.util.Set;


/**
 * Convert executionPlan string to executionplan object
 */
public class ExecutionPlanParser {
    private static final Logger log = Logger.getLogger(ExecutionPlanParser.class);

    private ExecutionPlanParser() {
    }

    public static ExecutionPlanDto executionPlanDtoParser(String executionPlan) {

        ExecutionPlanDto executionPlanDto = new ExecutionPlanDto();
        JSONObject jsonObject = new JSONObject(executionPlan);
        Gson gson = new Gson();
        executionPlanDto.setExecutionPlanName((String) jsonObject.get("executionPlanName"));
        JSONArray inputStreamArray = jsonObject.getJSONArray("inputStream");

        for (int i = 0; i < inputStreamArray.length(); i++) {
            StreamDefinitionDto streamDefinitionDto = gson.fromJson(String.valueOf(inputStreamArray.getJSONObject(i)), StreamDefinitionDto.class);
//            executionPlanDto.getInputStreamDtoMap().put(streamDefinitionDto.getStreamName(), streamDefinitionDto);
            executionPlanDto.setInputStreamDtoMap(streamDefinitionDto);

        }
        //set  output Stream Definition details
        JSONArray outputStreamArray = jsonObject.getJSONArray("OutputStream");

        for (int i = 0; i < outputStreamArray.length(); i++) {
            StreamDefinitionDto streamDefinitionDto = gson.fromJson(String.valueOf(outputStreamArray.getJSONObject(i)), StreamDefinitionDto.class);
            executionPlanDto.setOutputStreamDtoMap(streamDefinitionDto);
        }

        //set query details
        JSONArray queriesArray = jsonObject.getJSONArray("Queries");


        for (int i = 0; i < queriesArray.length(); i++) {
            Queries queries = gson.fromJson(String.valueOf(queriesArray.getJSONObject(i)), Queries.class);
//            executionPlanDto.getQueriesMap().put(queries.getQueryName(), queries);
            executionPlanDto.setQueriesMap(queries);
        }

        return executionPlanDto;
    }
}
