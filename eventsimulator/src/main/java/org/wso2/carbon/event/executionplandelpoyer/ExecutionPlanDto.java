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

package org.wso2.carbon.event.executionplandelpoyer;

import java.util.HashMap;
import java.util.Map;

/**
 * Create ExecutionPlanDto
 */
public class ExecutionPlanDto {
    private String executionPlanName;
    private Map<String, StreamDefinitionDto> inputStreamDtoMap = new HashMap<>();
    private Map<String, StreamDefinitionDto> outputStreamDtoMap = new HashMap<>();
    private Map<String, Queries> queriesMap = new HashMap<>();

    public String getExecutionPlanName() {
        return executionPlanName;
    }

    public void setInputStreamDtoMap(StreamDefinitionDto streamDefinitionDto) {
        this.inputStreamDtoMap.put(streamDefinitionDto.getStreamName(), streamDefinitionDto);
    }

    public void setOutputStreamDtoMap(StreamDefinitionDto streamDefinitionDto) {
        this.outputStreamDtoMap.put(streamDefinitionDto.getStreamName(), streamDefinitionDto);
    }

    public void setQueriesMap(Queries  queries ) {
        this.queriesMap.put(queries.getQueryName(), queries);;
    }

    public void setExecutionPlanName(String executionPlanName) {
        this.executionPlanName = executionPlanName;
    }

    public Map<String, StreamDefinitionDto> getInputStreamDtoMap() {
        return inputStreamDtoMap;
    }

    public Map<String, StreamDefinitionDto> getOutputStreamDtoMap() {
        return outputStreamDtoMap;
    }

    public Map<String, Queries> getQueriesMap() {
        return queriesMap;
    }


}

