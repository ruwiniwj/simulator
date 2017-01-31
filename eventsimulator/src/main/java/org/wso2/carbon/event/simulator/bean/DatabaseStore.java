package org.wso2.carbon.event.simulator.bean;

import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;

/**
 * DatabaseStore stores the database configurations
 */
public class DatabaseStore {
    /**
     * Concurrent HashMap hold the details of the database configurations
     * key: database configuration name
     * value: DatabaseFeedSimulationDto which holds database information
     *
     * @see DatabaseFeedSimulationDto
     */
    private ConcurrentHashMap<String, DatabaseFeedSimulationDto> databaseInfoMap = new ConcurrentHashMap<>();

    private static DatabaseStore databaseStore;

    /**
     * Create a singleton databaseStore object
     *
     * @return databaseStore
     */
    public static DatabaseStore getDatabaseStore() {
        if (databaseStore == null) {
            synchronized (DatabaseStore.class) {
                if (databaseStore == null) {
                    databaseStore = new DatabaseStore();
                }
            }
        }
        return databaseStore;
    }
}
