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
    private ResultSet resultSet;

    public DatabaseFeedSimulationDto() {  }

    public void setDatabaseConfigName(String databaseConfigName) {
        this.databaseConfigName = databaseConfigName;
    }

    public String getDatabaseConfigName() {
        return databaseConfigName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setUsername(String username){this.username=username;}

    public String getUsername(){return username;}

    public void setPassword(String password){this.password = password;}

    public String getPassword(){return password;}

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setColumnNamesAndTypes(HashMap<String,String> columnNamesAndTypes) {
        this.columnNamesAndTypes = columnNamesAndTypes;
    }

    public HashMap<String, String> getColumnNamesAndTypes() {
        return columnNamesAndTypes;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public String getStreamName() {
        return streamName;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public void setResultSet(ResultSet resultSet){this.resultSet = resultSet;}

    public ResultSet getResultSet(){return resultSet;}

}
