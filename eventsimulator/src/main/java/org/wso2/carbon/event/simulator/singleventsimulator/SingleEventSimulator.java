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
import org.wso2.carbon.event.executionplandelpoyer.Event;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;
import org.wso2.carbon.event.simulator.EventSimulator;
import org.wso2.carbon.event.simulator.bean.FeedSimulationStreamConfiguration;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.utils.EventConverter;
import org.wso2.carbon.event.simulator.utils.EventSender;

import java.util.Arrays;

/**
 * SingleEventSimulator simulates the deployed execution plan using single event.
 * It implements EventSimulator parentclass
 */
public class SingleEventSimulator implements EventSimulator {
    private static final Logger log = Logger.getLogger(SingleEventSimulator.class);
    private SingleEventDto streamConfiguration;

    /**
     * Initialize single event simulator for single event simulation process
     *
     * @param streamConfiguration
     */
    public SingleEventSimulator(SingleEventDto streamConfiguration) {
        this.streamConfiguration = streamConfiguration;
    }

    @Override
    public void pause() {
        // no need to pause
    }

    @Override
    public void resume() {
        // no need to pause
    }

    @Override
    public void stop() {
        // no need to stop
    }

    @Override
    public FeedSimulationStreamConfiguration getStreamConfiguration() {
        return streamConfiguration;
    }

    @Override
    public void run() {
        //attributeValue used to store values of attributes of an input stream
        String[] attributeValue = new String[streamConfiguration.getAttributeValues().size()];
        attributeValue = streamConfiguration.getAttributeValues().toArray(attributeValue);
        String streamName = streamConfiguration.getStreamName();
        Event event;
        try {
            //Convert attribute value as an Event
            event = EventConverter.eventConverter(streamName, attributeValue, ExecutionPlanDeployer.getInstance().getExecutionPlanDto());
            System.out.println("Input Event (Single feel)" + Arrays.deepToString(event.getEventData())); //TODO: 11/12/16 delete print statement

            //send created event to inputHandler for further process in siddhi
            EventSender.getInstance().sendEvent(event);
        } catch (EventSimulationException e) {
            log.error("Error occurred : Failed to send an event" + e.getMessage());
        }
    }
}
