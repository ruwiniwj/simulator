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
package org.wso2.carbon.event.simulator.singleventsimulator;

import org.apache.log4j.Logger;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;
import org.wso2.carbon.event.simulator.EventSimulator;
import org.wso2.carbon.event.executionplandelpoyer.Event;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.utils.EventConverter;

import java.util.Arrays;

/**
 * SingleEventSimulator simulates the deployed execution plan using single event.
 * It implements EventSimulator parentclass
 */
public class SingleEventSimulator implements EventSimulator {
    private static final Logger log = Logger.getLogger(SingleEventSimulator.class);

    /**
     * Initialize single event simulator for single event simulation process
     */
    public SingleEventSimulator() {
    }

    /**
     * send the created event to siddhi InputHandler of particular input stream
     *
     * @param streamName Stream Name
     * @param event      Event Object
     */

    @Override
    public void send(String streamName, Event event) {
        try {
            ExecutionPlanDeployer.getInstance().getInputHandlerMap().get(streamName).send(event.getEventData());
        } catch (InterruptedException e) {
            log.error("Error occurred during send event :" + e.getMessage());
        }
    }

    /**
     * Create event as stated in single event simulation configuration
     * send created event to inputHandler for further process in siddhi
     * <p>
     * Initialize new event
     *
     * @param singleEventSimulationConfig single Event Configuration
     * @return true if event send successfully ; false if fails
     */
    public void send(SingleEventDto singleEventSimulationConfig) {
        //attributeValue used to store values of attributes of an input stream
        String[] attributeValue = new String[singleEventSimulationConfig.getAttributeValues().size()];
        attributeValue = singleEventSimulationConfig.getAttributeValues().toArray(attributeValue);
        String streamName = singleEventSimulationConfig.getStreamName();
        Event event;
        try {
            //Convert attribute value as an Event
            event = EventConverter.eventConverter(streamName, attributeValue, ExecutionPlanDeployer.getInstance().getExecutionPlanDto());
            System.out.println("Input Event " + Arrays.deepToString(event.getEventData())); //TODO: 11/12/16 delete print statement

            //send created event to inputHandler for further process in siddhi
            send(streamName, event);
        } catch (EventSimulationException e) {
            log.error("Error occurred : Failed to send an event" + e.getMessage());
        }

    }


    @Override
    public void resume() {

    }

}
