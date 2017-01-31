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
package org.wso2.carbon.event.simulator.utils;


import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDto;
import org.wso2.carbon.event.simulator.constants.EventSimulatorConstants;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.executionplandelpoyer.Event;

import java.util.List;


/**
 * Event converter convert the given attribute list as event
 */
public class EventConverter {
    /**
     * Initialize the EventConverter
     */
    private EventConverter() {
    }

    /**
     * Initialize Event
     * Convert convert the given attribute list as event
     *
     * @param streamName    Stream Name
     * @param dataList      list of attribute values to be converted to as event data
     * @param executionPlan Execution plan
     * @return created Event
     */
    public static Event eventConverter(String streamName, String[] dataList, ExecutionPlanDto executionPlan) {
        Event event = new Event();
        event.setStreamName(streamName);

        Object[] eventData = new Object[executionPlan.getInputStreamDtoMap().get(streamName).getStreamAttributeDtos().size()];
        List<org.wso2.carbon.event.executionplandelpoyer.StreamAttributeDto> streamAttributeDto =
                executionPlan.getInputStreamDtoMap().get(streamName).getStreamAttributeDtos();

        //Convert attribute values according to attribute type in stream definition
        for (int j = 0; j < dataList.length; j++) {
            String type = streamAttributeDto.get(j).getAttributeType();
            switch (type) {
                case EventSimulatorConstants.ATTRIBUTETYPE_INTEGER:
                    try {
                        eventData[j] = Integer.parseInt(String.valueOf(dataList[j]));
                    } catch (NumberFormatException e) {
                        throw new EventSimulationException("Incorrect value types for the attribute - " +
                                streamAttributeDto.get(j).getAttributeName() +
                                ", expected" + streamAttributeDto.get(j).getAttributeType() + " : " + e.getMessage());
                    }
                    break;
                case EventSimulatorConstants.ATTRIBUTETYPE_LONG:
                    try {
                        eventData[j] = Long.parseLong(String.valueOf(dataList[j]));
                    } catch (NumberFormatException e) {
                        throw new EventSimulationException("Incorrect value types for the attribute - " +
                                streamAttributeDto.get(j).getAttributeName() +
                                ", expected" + streamAttributeDto.get(j).getAttributeType() + " : " + e.getMessage());
                    }
                    break;
                case EventSimulatorConstants.ATTRIBUTETYPE_FLOAT:
                    try {
                        eventData[j] = Float.parseFloat(String.valueOf(dataList[j]));
                    } catch (NumberFormatException e) {
                        throw new EventSimulationException("Incorrect value types for the attribute - " +
                                streamAttributeDto.get(j).getAttributeName() +
                                ", expected" + streamAttributeDto.get(j).getAttributeType() + " : " + e.getMessage());
                    }
                    break;
                case EventSimulatorConstants.ATTRIBUTETYPE_DOUBLE:
                    try {
                        eventData[j] = Double.parseDouble(String.valueOf(dataList[j]));
                    } catch (NumberFormatException e) {
                        throw new EventSimulationException("Incorrect value types for the attribute - " +
                                streamAttributeDto.get(j).getAttributeName() +
                                ", expected" + streamAttributeDto.get(j).getAttributeType() + " : " + e.getMessage());
                    }
                    break;
                case EventSimulatorConstants.ATTRIBUTETYPE_STRING:
                    eventData[j] = dataList[j];
                    break;
                case EventSimulatorConstants.ATTRIBUTETYPE_BOOLEAN:
                    if (String.valueOf(dataList[j]).equalsIgnoreCase("true") || String.valueOf(dataList[j]).equalsIgnoreCase("false")) {
                        eventData[j] = Boolean.parseBoolean(String.valueOf(dataList[j]));
                    } else {
                        throw new EventSimulationException("Incorrect value types for the attribute - " +
                                streamAttributeDto.get(j).getAttributeName() +
                                ", expected" + streamAttributeDto.get(j).getAttributeType() + " : " +
                                new IllegalArgumentException().getMessage());
                    }
                    break;
            }

        }
        event.setEventData(eventData);
        return event;
    }
}
