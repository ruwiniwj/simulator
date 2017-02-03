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


import org.apache.log4j.Logger;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.util.Arrays;
import java.util.HashMap;

import java.util.Map;

/**
 * Deploy execution plan
 */
public class ExecutionPlanDeployer {
    private static final Logger log = Logger.getLogger(ExecutionPlanDeployer.class);
    private static ExecutionPlanDeployer executionPlanDeployer;
    private ExecutionPlanDto executionPlanDto;
    private Map<String, InputHandler> inputHandlerMap = new HashMap<>();
    private static SiddhiManager siddhiManager;
    private ExecutionPlanRuntime executionPlanRuntime;

    private ExecutionPlanDeployer() {

    }

    public ExecutionPlanDto getExecutionPlanDto() {
        return executionPlanDto;
    }

    public Map<String, InputHandler> getInputHandlerMap() {
        return inputHandlerMap;
    }

    public ExecutionPlanRuntime getExecutionPlanRuntime() {
        return executionPlanRuntime;
    }


    public static ExecutionPlanDeployer getInstance() {
        if (executionPlanDeployer == null) {
            synchronized (ExecutionPlanDeployer.class) {
                if (executionPlanDeployer == null) {
                    executionPlanDeployer = new ExecutionPlanDeployer();
                }
            }
        }
        return executionPlanDeployer;
    }


    /**
     * Deploy the execution plan
     *
     * @param executionPlanDto Execution Plan Details
     */
    public void deployExecutionPlan(ExecutionPlanDto executionPlanDto) {
        try {
            this.siddhiManager = new SiddhiManager();
            String executionPlan = createExecutionplan(executionPlanDto);
            this.executionPlanDto = executionPlanDto;
            this.executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(executionPlan);
            this.inputHandlerMap = createInputHandlerMap(executionPlanDto, executionPlanRuntime);
            ExecutionPlanDeployer.getInstance().getExecutionPlanRuntime().start();
            addStreamCallback();
            log.info("Execution Plan is deployed Successfully");
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw new EventSimulationException("Error occurred during execution plan deployment" );
        }
    }

    /**
     * Create siddhi Execution Plan
     * <p>
     * In this class concatenate all input streams definition and queries as databaseFeedSimulation Execution plan string
     * Siddhi should recognize it without any error
     *
     * @param executionPlanDto Execution Plan Details
     * @return Execution plan
     */
    private String createExecutionplan(ExecutionPlanDto executionPlanDto) {
        String streams = "";
        String setOfQuery = "";
        for (Object o : executionPlanDto.getInputStreamDtoMap().entrySet()) {
            Map.Entry<String, StreamDefinitionDto> stream = (Map.Entry) o;
            streams += stream.getValue().getStreamDefinition();
            // streamIterator.remove(); // avoids databaseFeedSimulation ConcurrentModificationException
        }

        for (Object o : executionPlanDto.getQueriesMap().entrySet()) {
            Map.Entry<String, Queries> query = (Map.Entry) o;
            setOfQuery += query.getValue().getQueryDefinition();
        }

        return streams + setOfQuery;
    }

    /**
     * Create the input handler Map to each input streams
     *
     * @param executionPlanDto     Execution plan details
     * @param executionPlanRuntime Execution plan runtime
     * @return inputHandlerMap
     */
    private Map<String, InputHandler> createInputHandlerMap(ExecutionPlanDto executionPlanDto, ExecutionPlanRuntime executionPlanRuntime) {
        Map<String, InputHandler> inputHandlerMap = new HashMap<>();
        for (Object o : executionPlanDto.getInputStreamDtoMap().entrySet()) {
            Map.Entry stream = (Map.Entry) o;
            inputHandlerMap.put((String) stream.getKey(), executionPlanRuntime.getInputHandler((String) stream.getKey()));
            // streamIterator.remove(); // avoids databaseFeedSimulation ConcurrentModificationException
        }
        return inputHandlerMap;
    }

    private void addStreamCallback() {
        this.executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                System.out.println("Output Event: " + Arrays.deepToString(events));
            }
        });
    }

}
