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

package org.wso2.carbon.event.simulator.bean;

import org.wso2.carbon.event.simulator.csvFeedSimulation.CSVFileSimulationDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.RandomDataSimulationDto;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import scala.util.parsing.combinator.testing.Str;

/**
 * FeedSimulationStreamConfiguration class represents the feed simulation configuration for an input stream.
 * This is an abstract class, CSVFileSimulationDto and RandomDataSimulationDto extends this parent class
 *
 * @see CSVFileSimulationDto
 * @see RandomDataSimulationDto
 * @see DatabaseFeedSimulationDto
 */
public abstract class FeedSimulationStreamConfiguration {
    /**
     * Simulation type for an input stream
     * It can be
     * 1. RandomDataSimulation
     * 2. FileFeedSimulation
     * 3. DatabaseSimulation
     * These values are constants and choose by user
     */
    private String simulationType;

    private String timestampAttribute;

    // TODO: 21/12/16 add stream here
    public String getSimulationType() {
        return simulationType;
    }

    public void setSimulationType(String simulationType) {
        this.simulationType = simulationType;
    }

    public String getTimestampAttribute() { return timestampAttribute;}

    public void setTimestampAttribute(String timestampAttribute) { this.timestampAttribute = timestampAttribute; }


}
