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
import org.wso2.carbon.event.simulator.EventSimulator;
import org.wso2.carbon.event.simulator.bean.FeedSimulationDto;
import org.wso2.carbon.event.simulator.bean.FeedSimulationStreamConfiguration;
import org.wso2.carbon.event.simulator.constants.EventSimulatorConstants;
import org.wso2.carbon.event.simulator.csvFeedSimulation.CSVFileSimulationDto;
import org.wso2.carbon.event.simulator.csvFeedSimulation.core.CSVFeedEventSimulator;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.core.DatabaseFeedSimulator;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.RandomDataSimulationDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.core.RandomDataEventSimulator;
import org.wso2.carbon.event.simulator.singleventsimulator.SingleEventDto;
import org.wso2.carbon.event.simulator.singleventsimulator.SingleEventSimulator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * EventSimulatorPoolExecutor starts the simulation execution for single Event and
 * Feed Simulation
 */
// TODO: 2/4/17 ExecutionPlanDeployer.getInstance().getExecutionPlanRuntime().shutdown(); goes in beforeshutdown?
public class EventSimulatorPoolExecutor extends ThreadPoolExecutor {
    private static final Logger log = Logger.getLogger(EventSimulatorPoolExecutor.class);
    private boolean isPaused;
    private ReentrantLock pauseLock = new ReentrantLock();
    private Condition unpaused = pauseLock.newCondition();
    private List<EventSimulator> simulators = new ArrayList<>();

    public EventSimulatorPoolExecutor(FeedSimulationDto configuration, int nThreads) {
        super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        for (FeedSimulationStreamConfiguration streamConfiguration : configuration.getStreamConfigurationList()) {
            switch (streamConfiguration.getSimulationType()) {
                case EventSimulatorConstants.RANDOM_DATA_SIMULATION:
                    simulators.add(new RandomDataEventSimulator((RandomDataSimulationDto) streamConfiguration));
                    break;
                case EventSimulatorConstants.FILE_FEED_SIMULATION:
                    simulators.add(new CSVFeedEventSimulator((CSVFileSimulationDto) streamConfiguration));
                    break;
                case EventSimulatorConstants.DATABASE_FEED_SIMULATION:
                    simulators.add(new DatabaseFeedSimulator((DatabaseFeedSimulationDto) streamConfiguration));
                    break;
                case EventSimulatorConstants.SINGLE_EVENT_SIMULATION:
                    simulators.add(new SingleEventSimulator((SingleEventDto) streamConfiguration));
                    break;
                default:
                    break;
            }
        }
        simulators.forEach(this::execute);
    }

    public static EventSimulatorPoolExecutor newEventSimulatorPool(FeedSimulationDto configuration, int nThreads) {
        return new EventSimulatorPoolExecutor(configuration, nThreads);
    }

    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        pauseLock.lock();
        try {
            while (isPaused) unpaused.await();
        } catch (InterruptedException ie) {
            t.interrupt();
        } finally {
            pauseLock.unlock();
        }
        EventSender.getInstance().addSimulator(((EventSimulator) r).getStreamConfiguration().getStreamName());
    }

    protected void afterExecute(Runnable r, Throwable t) {
        try {
            EventSender.getInstance().removeSimulator(((EventSimulator) r).getStreamConfiguration().getStreamName());
        } finally {
            super.afterExecute(r, t);
        }
    }

    public void pause() {
        pauseLock.lock();
        try {
            isPaused = true;
            simulators.forEach(EventSimulator::pause);
        } finally {
            pauseLock.unlock();
        }
    }

    public void resume() {
        pauseLock.lock();
        try {
            isPaused = false;
            simulators.forEach(EventSimulator::resume);
            unpaused.signalAll();
        } finally {
            pauseLock.unlock();
        }
    }

    public void stop() {
        simulators.forEach(EventSimulator::stop);
        shutdownNow();
    }

    // TODO: 2/4/17 make them available via REST service to get state of executor
    public boolean isRunning() {
        return !isPaused;
    }

    public boolean isPaused() {
        return isPaused;
    }
}



