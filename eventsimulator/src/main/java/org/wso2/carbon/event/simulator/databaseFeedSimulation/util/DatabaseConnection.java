package org.wso2.carbon.event.simulator.databaseFeedSimulation.util;

import org.apache.log4j.Logger;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import org.wso2.carbon.event.simulator.exception.DatabaseConnectionException;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by ruwini on 1/29/17.
 */

/** DatabaseConnection is a utility class that loads the driver, connects to the database, executes the SELECT query
 * and returns the result set
 */

public class DatabaseConnection {

    private static final Logger log = Logger.getLogger(DatabaseConnection.class);

    private static String driver = "com.mysql.jdbc.Driver";
    private String URL = "jdbc:mysql://localhost:3306/";
    private Connection dbConnection;
    private String dataSourceLocation;
    private String databaseName;
    private String username;
    private String password;
    private String tableName;
    private HashMap<String,String> columnNamesAndTypes;


    public DatabaseConnection(){}

    public ResultSet getDatabaseEventItems(DatabaseFeedSimulationDto databaseFeedSimulationDto){

        this.databaseName = databaseFeedSimulationDto.getDatabaseName();
        this.dataSourceLocation = this.URL+databaseName;
        this.username = databaseFeedSimulationDto.getUsername();
        this.password = databaseFeedSimulationDto.getPassword();
        this.tableName = databaseFeedSimulationDto.getTableName();
        this.columnNamesAndTypes = databaseFeedSimulationDto.getColumnNamesAndTypes();
        List<String> columns = new ArrayList<>(columnNamesAndTypes.keySet());
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

       try{
           this.dbConnection = connectToDatabase(dataSourceLocation,username,password);
           if(!dbConnection.isClosed() || dbConnection != null)
           {
               if(checkTableExists(tableName,databaseName))
               {
               String query = prepareSQLstatement(tableName,columns);
               preparedStatement = dbConnection.prepareStatement(query);
               resultSet = preparedStatement.executeQuery();
               }
           }
           return resultSet;
       }
       catch (SQLException e)
       {
           throw new DatabaseConnectionException("Error : " + e.getMessage());
       }
//        todo R close the resultset, prepared statement and connection
    /*   finally {
           try { resultSet.close(); } catch (Exception e) {}
           try { preparedStatement.close(); } catch (Exception e) {}
           try { dbConnection.close(); } catch (Exception e) {}
       }*/

    }

    /** This method loads the JDBC driver and returns a database connection*/

    private Connection connectToDatabase(String dataSourceLocation, String username, String password ){
        try {
            Class.forName(driver).newInstance();
            Connection connection = DriverManager.getConnection(dataSourceLocation,username,password);
            System.out.println("success");
            return connection;
        }
        /*
        When loading the driver either one of the following exceptions may occur
        1. ClassNotFoundException
        2. IllegalAccessException
        3. InstantiationException

        Establishing a database connection may throw an SQLException
        */
        catch (SQLException e)
        {
            throw new DatabaseConnectionException( " Error occurred while connecting to database : " + e.getMessage());
        }
        catch (Exception e)
        {
            throw new DatabaseConnectionException(" Error occurred when loading driver : " + e.getMessage());
        }
    }


    private boolean checkTableExists(String tableName,String databaseName)
    {
        try {
            DatabaseMetaData metaData = dbConnection.getMetaData();
            ResultSet tableResults = metaData.getTables(null, null, tableName, null);
            if(tableResults.isBeforeFirst())
                return true;
            else
                throw new DatabaseConnectionException("Table" + tableName + "does not exist in " + databaseName);
        }
        catch (SQLException e)
        {
           throw new DatabaseConnectionException(e.getMessage());
        }
    }

    private String prepareSQLstatement(String tableName, List<String> columns)
    {
        if(columns.contains(null) || columns.contains(""))
        {
            throw new DatabaseConnectionException(" Column names cannot contain null values or empty strings");
        }
        else
        {
            String columnNames = String.join(",",columns);
            String query = String.format("SELECT %s FROM %s;",columnNames,tableName);
            return query;

           /* StringBuilder query = new StringBuilder("SELECT ");
            query.append(String.join(",",columns));
            query.append(" FROM " + tableName + ";");
            return query.toString();*/
        }
    }

}
