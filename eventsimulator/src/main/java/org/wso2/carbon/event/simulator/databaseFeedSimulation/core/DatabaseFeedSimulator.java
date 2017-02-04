package org.wso2.carbon.event.simulator.databaseFeedSimulation.core;


import org.apache.log4j.Logger;
import org.wso2.carbon.event.executionplandelpoyer.Event;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDto;
import org.wso2.carbon.event.executionplandelpoyer.StreamAttributeDto;
import org.wso2.carbon.event.executionplandelpoyer.StreamDefinitionDto;
import org.wso2.carbon.event.simulator.EventSimulator;
import org.wso2.carbon.event.simulator.bean.FeedSimulationStreamConfiguration;
import org.wso2.carbon.event.simulator.constants.EventSimulatorConstants;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.util.DatabaseConnection;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.utils.EventConverter;
import org.wso2.carbon.event.simulator.utils.EventSender;
import org.wso2.carbon.event.simulator.utils.QueuedEvent;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This simulator simulates the execution plan by sending events from data in a database.
 * <p>
 * This simulator class implements EventSimulator Interface
 */
public class DatabaseFeedSimulator implements EventSimulator {
    private static final Logger log = Logger.getLogger(DatabaseFeedSimulator.class);
    private final Object lock = new Object();
    private DatabaseFeedSimulationDto streamConfiguration;
    private volatile boolean isPaused = false;
    private volatile boolean isStopped = false;

    /**
     * Initialize DatabaseFeedSimulator to star simulation
     *
     * @param streamConfiguration
     */
    public DatabaseFeedSimulator(DatabaseFeedSimulationDto streamConfiguration) {
        this.streamConfiguration = streamConfiguration;
    }

    @Override
    public void pause() {
        isPaused = true;
    }

    @Override
    public void resume() {
        isPaused = false;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    @Override
    public void stop() {
        isPaused = true;
        isStopped = true;
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    @Override
    public FeedSimulationStreamConfiguration getStreamConfiguration() {
        return streamConfiguration;
    }

    @Override
    public void run() {
        sendEvent(ExecutionPlanDeployer.getInstance().getExecutionPlanDto(), streamConfiguration);
    }

    /**
     * Create and send events using data in database
     *
     * @param executionPlanDto          : ExecutionPlanDto object containing the execution plan
     * @param databaseFeedConfiguration : DatabaseFeedSimulationDto object containing database simulation configuration
     */

    private void sendEvent(ExecutionPlanDto executionPlanDto, DatabaseFeedSimulationDto databaseFeedConfiguration) {
        int delay = databaseFeedConfiguration.getDelay();
        StreamDefinitionDto streamDefinitionDto = executionPlanDto.getInputStreamDtoMap().get(databaseFeedConfiguration.getStreamName());
        List<StreamAttributeDto> streamAttributeDtos = streamDefinitionDto.getStreamAttributeDtos();
        List<String> columnNames = new ArrayList<>(databaseFeedConfiguration.getColumnNamesAndTypes().keySet());
        String timestampAttribute = databaseFeedConfiguration.getTimestampAttribute();
        boolean valid = columnValidation(columnNames, streamAttributeDtos);

        if (valid) {
            DatabaseConnection databaseConnection;
            ResultSet resultSet;
            databaseConnection = new DatabaseConnection();

            try {
                resultSet = databaseConnection.getDatabaseEventItems(databaseFeedConfiguration);

                if (!resultSet.isBeforeFirst()) {
                    throw new EventSimulationException(" Table " + databaseFeedConfiguration.getTableName() + " contains " +
                            " no entries for the columns specified.");
                }

                while (resultSet.next()) {
                    if (!isPaused) {
                        String[] attributeValues = new String[streamAttributeDtos.size()];
                        for (int i = 0; i < streamAttributeDtos.size(); i++) {
                            if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_STRING)) == 0) {
                                attributeValues[i] = resultSet.getString(streamAttributeDtos.get(i).getAttributeName());
                            } else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_INTEGER)) == 0) {
                                attributeValues[i] = String.valueOf(resultSet.getInt(streamAttributeDtos.get(i).getAttributeName()));
                            } else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_DOUBLE)) == 0) {
                                attributeValues[i] = String.valueOf(resultSet.getDouble(streamAttributeDtos.get(i).getAttributeName()));
                            } else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_FLOAT)) == 0) {
                                attributeValues[i] = String.valueOf(resultSet.getFloat(streamAttributeDtos.get(i).getAttributeName()));
                            } else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_BOOLEAN)) == 0) {
                                attributeValues[i] = String.valueOf(resultSet.getBoolean(streamAttributeDtos.get(i).getAttributeName()));
                            }
                        }

                        Event event = EventConverter.eventConverter(databaseFeedConfiguration.getStreamName(), attributeValues, executionPlanDto);
                        System.out.println("Input Event (Database feed)" + Arrays.deepToString(event.getEventData()));

                        if (databaseFeedConfiguration.getTimestampAttribute().isEmpty()) {
                            EventSender.getInstance().sendEvent(event);
                        } else {
                            EventSender.getInstance().sendEvent(new QueuedEvent(resultSet.getLong(timestampAttribute), event));
                        }

                        if (delay > 0) {
                            Thread.sleep(delay);
                        }
                    } else if (isStopped) {
                        break;
                    } else {
                        synchronized (lock) {
                            try {
                                lock.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                continue;
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                log.error("Error occurred when generating event : " + e.getMessage());
            } catch (InterruptedException e) {
                log.error("Error occurred when sending event : " + e.getMessage());
            } finally {
                if (databaseConnection != null) {
                    databaseConnection.closeConnection();
                }
            }
        }
    }


    /**
     * columnValidation method is used to validate the column names provided.
     * It performs the following validations
     * 1. The column names are not null or empty
     * 2. The number of columns provided is equal to the number of attributes in the stream
     * 3. Each attribute has a matching column name
     */

    private boolean columnValidation(List<String> columnNames, List<StreamAttributeDto> streamAttributeDtos) {

        if (columnNames.contains(null) || columnNames.contains("")) {
            throw new EventSimulationException(" Column names cannot contain null values or empty strings");
        }

        if (columnNames.size() != streamAttributeDtos.size()) {
            throw new EventSimulationException(" Stream requires " + streamAttributeDtos.size() + " attribute. Number of columns " +
                    " specified is " + columnNames.size());
        }

        for (int i = 0; i < streamAttributeDtos.size(); i++) {
            boolean columnAvailable = false;
            for (int j = 0; j < columnNames.size(); j++) {
                if ((String.valueOf(streamAttributeDtos.get(i).getAttributeName())).compareToIgnoreCase(String.valueOf(columnNames.get(j))) == 0) {
                    columnAvailable = true;
                    break;
                }
            }
            if (columnAvailable) {
                continue;
            } else {
                log.error("Column required for attribute : " + streamAttributeDtos.get(i).getAttributeName());
                return false;
            }
        }
        return true;
    }
}
