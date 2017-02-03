package org.wso2.carbon.event.simulator.databaseFeedSimulation;


import org.wso2.carbon.event.simulator.bean.FeedSimulationStreamConfiguration;

import java.sql.ResultSet;
import java.util.HashMap;

/**
 * DatabaseFeedSimulationDto returns configuration for database simulation.
 */
public class DatabaseFeedSimulationDto extends FeedSimulationStreamConfiguration {

    private String databaseConfigName;
    private String databaseName;
    private String username;
    private String password;
    private String tableName;
    private HashMap<String,String> columnNamesAndTypes;
    private String streamName;
    private int delay;

    public DatabaseFeedSimulationDto() {  }

    public String getDatabaseConfigName() { return databaseConfigName; }

    public void setDatabaseConfigName(String databaseConfigName) {
        this.databaseConfigName = databaseConfigName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getUsername(){return username;}

    public void setUsername(String username){this.username=username;}

    public String getPassword(){return password;}

    public void setPassword(String password){this.password = password;}

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public HashMap<String, String> getColumnNamesAndTypes() {
        return columnNamesAndTypes;
    }

    public void setColumnNamesAndTypes(HashMap<String,String> columnNamesAndTypes) {
        this.columnNamesAndTypes = columnNamesAndTypes;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

}
