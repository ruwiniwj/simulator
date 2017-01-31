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


import org.apache.log4j.Logger;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;
import org.wso2.carbon.event.simulator.bean.FeedSimulationDto;
import org.wso2.carbon.event.simulator.bean.FeedSimulationStreamConfiguration;
import org.wso2.carbon.event.simulator.constants.EventSimulatorConstants;
import org.wso2.carbon.event.simulator.csvFeedSimulation.CSVFileSimulationDto;
import org.wso2.carbon.event.simulator.csvFeedSimulation.core.CSVFeedEventSimulator;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.core.DatabaseFeedSimulator;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.RandomDataSimulationDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.core.RandomDataEventSimulator;
import org.wso2.carbon.event.simulator.singleventsimulator.SingleEventDto;
import org.wso2.carbon.event.simulator.singleventsimulator.SingleEventSimulator;


/**
 * EventSimulatorServiceExecutor starts the simulation execution for single Event and
 * Feed Simulation
 */

public class EventSimulatorServiceExecutor{
    private static final Logger log = Logger.getLogger(EventSimulatorServiceExecutor.class);

    /**
     * running used to indicate simulation process
     * In the case of single event simulator If running is true it won't allowed another single
     * Event simulation process until previous one finishes it's task
     */
    private volatile boolean running = false;

    /**
     * RandomDataEventSimulator
     */
    private RandomDataEventSimulator randomDataEventSimulator;

    /**
     * CSVFeedEventSimulator
     */
    private CSVFeedEventSimulator csvFeedEventSimulator;

    /**
     * DatabaseFeedSimulator
     * */
    private DatabaseFeedSimulator databaseFeedSimulator;

    /**
     * Initialize the SingleEventSimulator
     * call send function to start the single event simulation
     *
     * @param singleEventDto SingleEventSimulationConfiguration
     */
    public void simulateSingleEvent(SingleEventDto singleEventDto) {
        if (!running) {
            synchronized (this) {
                if (!running) {
                    SingleEventSimulator singleEventSimulator = new SingleEventSimulator();
                    singleEventSimulator.send(singleEventDto);
                }
            }
        }
        log.info("Event is send success Fully");
    }

    /**
     * Creates the thread pool for feed Simulation
     *
     * @param feedSimulationConfig FeedSimulationDto
     */
    public void simulateFeedSimulation(FeedSimulationDto feedSimulationConfig) {
        if (!running) {
                synchronized (this) {
                    if (!running) {
                        running = true;
                        int noOfStream = feedSimulationConfig.getStreamConfigurationList().size();
                        //creating databaseFeedSimulation thread pool for feed simulation
                        for (int i = 0; i < noOfStream; i++) {
                            SimulationStarter simulationStarter = new SimulationStarter(feedSimulationConfig.getStreamConfigurationList().get(i));
                            //calling execute method of ExecutorService
//                        executor.execute(simulationStarter);
                            new Thread(simulationStarter).start();
                        }
                    }
                }
            }
    }


    private class SimulationStarter implements Runnable {
        FeedSimulationStreamConfiguration streamConfiguration;

        SimulationStarter(FeedSimulationStreamConfiguration streamConfiguration) {
            this.streamConfiguration = streamConfiguration;
        }

        /**
         * When an object implementing interface <code>Runnable</code> is used
         * to create databaseFeedSimulation thread, starting the thread causes the object's
         * <code>run</code> method to be called in that separately executing
         * thread.
         * <p>
         * The general contract of the method <code>run</code> is that it may
         * take any action whatsoever.
         *
         * @see Thread#run()
         */
        @Override
        public void run() throws EventSimulationException {
            try {
                if (streamConfiguration.getSimulationType().compareTo(EventSimulatorConstants.RANDOM_DATA_SIMULATION) == 0) {
                    randomDataEventSimulator = new RandomDataEventSimulator();
                    // TODO: 21/12/16 init or start
                    randomDataEventSimulator.send((RandomDataSimulationDto) streamConfiguration);
                } else if (streamConfiguration.getSimulationType().compareTo(EventSimulatorConstants.FILE_FEED_SIMULATION) == 0) {
                    csvFeedEventSimulator = new CSVFeedEventSimulator();
                    csvFeedEventSimulator.send((CSVFileSimulationDto) streamConfiguration);
                }
                // TODO: 14/12/16 For Database simulation
                else if (streamConfiguration.getSimulationType().compareTo(EventSimulatorConstants.DATABASE_FEED_SIMULATION) == 0)
                {
                    databaseFeedSimulator = new DatabaseFeedSimulator();
                    databaseFeedSimulator.send((DatabaseFeedSimulationDto) streamConfiguration);
                }

            } catch (RuntimeException e) {
                throw new RuntimeException("Error while simulation :" + e.getMessage());
            }

        }
    }

    /**
     * Stop the simulation process
     */
    public void stop() {
        try {
            if (running) {
                synchronized (this) {
                    if (running) {
                        if (randomDataEventSimulator != null) {
                            RandomDataEventSimulator.isStopped = true;
                            randomDataEventSimulator = null;
                        }
                        if (csvFeedEventSimulator != null) {
                            CSVFeedEventSimulator.isStopped = true;
                            csvFeedEventSimulator = null;
                        }
                        if (databaseFeedSimulator != null)
                        {
                            DatabaseFeedSimulator.isStopped = true;
                            databaseFeedSimulator = null;
                        }
                        this.running = false;
                        ExecutionPlanDeployer.getInstance().getExecutionPlanRuntime().shutdown();
                        log.info("Feed Simulation process is stop");
                    }
                }
            }
        }catch (EventSimulationException e){
            throw new EventSimulationException("Error occurred during stopping Feed simulation process");
        }
    }

    /**
     * pause the simulation process
     */
    public void pause() {
        try{
        if (running) {
            if (randomDataEventSimulator != null) {
                RandomDataEventSimulator.isPaused = true;
            }
            if (csvFeedEventSimulator != null) {
                CSVFeedEventSimulator.isPaused = true;
            }
            if (databaseFeedSimulator != null)
            {
                DatabaseFeedSimulator.isPaused = true;
            }
            log.info("Event Simulation process is paused");
        }
        }catch (EventSimulationException e){
            throw new EventSimulationException("Error occurred during pausing Feed simulation process");
        }

    }

    /**
     * resume the simulation process
     */
    public void resume() {
        try {
            if (running) {
                if (randomDataEventSimulator != null) {
                    RandomDataEventSimulator.isPaused = false;
                    randomDataEventSimulator.resume();
                }
                if (csvFeedEventSimulator != null) {
                    CSVFeedEventSimulator.isPaused = false;
                    csvFeedEventSimulator.resume();
                }
                if (databaseFeedSimulator != null)
                {
                    DatabaseFeedSimulator.isPaused = false;
                    databaseFeedSimulator.resume();
                }
                System.out.println("Event Simulation process is Resumed");
            }
        }catch (EventSimulationException e){
            throw new EventSimulationException("Error occurred during resuming Feed simulation process");
        }
    }

}



