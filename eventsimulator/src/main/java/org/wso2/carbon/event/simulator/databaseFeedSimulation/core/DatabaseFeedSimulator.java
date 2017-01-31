package org.wso2.carbon.event.simulator.databaseFeedSimulation.core;


import org.apache.log4j.Logger;
import org.wso2.carbon.event.executionplandelpoyer.*;
import org.wso2.carbon.event.simulator.EventSimulator;
import org.wso2.carbon.event.simulator.constants.EventSimulatorConstants;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.utils.EventConverter;

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
public class DatabaseFeedSimulator implements EventSimulator{

    private static final Logger log = Logger.getLogger(DatabaseFeedSimulator.class);

    /**
     *Flag used to stop simulation
     **/

    public volatile static boolean isStopped = false;

    /**
     * Flag used to pause simulation
     **/

    public volatile static boolean isPaused = false;

    /**
     * Initialize DatabaseFeedSimulator to star simulation
     **/

    public DatabaseFeedSimulator(){}

    /**
     * send created event to siddhi input handler
     *
     * @param streamName stream name
     * @param  event created event
     * */



    @Override
    public void send(String streamName, Event event) {
        try{
            /*
            get the input handler for particular input stream Name and send the event to that input handler
             */
            ExecutionPlanDeployer.getInstance().getInputHandlerMap().get(streamName).send(event.getEventData());

        }catch (InterruptedException e)
        {
            log.error("Error occurred when sending event :" + e.getMessage());
        }


    }

    public boolean send(DatabaseFeedSimulationDto databaseFeedConfiguration)
    {
        synchronized (this)
        {
            sendEvent(ExecutionPlanDeployer.getInstance().getExecutionPlanDto(), databaseFeedConfiguration);
        }
        return true;
    }

    @Override
    public void resume() {
        synchronized (this)
        {
            this.notifyAll();
        }

    }
    private void sendEvent(ExecutionPlanDto executionPlanDto, DatabaseFeedSimulationDto databaseFeedConfiguration) {
        int delay = databaseFeedConfiguration.getDelay();
        ResultSet resultSet = databaseFeedConfiguration.getResultSet();
        StreamDefinitionDto streamDefinitionDto = executionPlanDto.getInputStreamDtoMap().get(databaseFeedConfiguration.getStreamName());
        List<StreamAttributeDto> streamAttributeDtos = streamDefinitionDto.getStreamAttributeDtos();

        try {
            while (resultSet.next())
            {
              synchronized (this)
              {
                  if(isStopped)
                  {
                      isStopped=false;
                      break;
                  }
                  if (isPaused)
                  {
                      this.wait();
                  }

                  String[] attributeValues = new String[streamAttributeDtos.size()];
                  for (int i =0; i<streamAttributeDtos.size(); i++)
                  {
                      if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_STRING)) == 0)
                      {
                          attributeValues[i] = resultSet.getString(streamAttributeDtos.get(i).getAttributeName());
                      }
                      else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_INTEGER)) == 0)
                      {
                          attributeValues[i] = String.valueOf(resultSet.getInt(streamAttributeDtos.get(i).getAttributeName()));
                      }
                      else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_DOUBLE)) == 0)
                      {
                          attributeValues[i] = String.valueOf(resultSet.getDouble(streamAttributeDtos.get(i).getAttributeName()));
                      }
                      else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_FLOAT)) == 0)
                      {
                          attributeValues[i] = String.valueOf(resultSet.getFloat(streamAttributeDtos.get(i).getAttributeName()));
                      }
                      else if ((streamAttributeDtos.get(i).getAttributeType().compareTo(EventSimulatorConstants.ATTRIBUTETYPE_BOOLEAN)) == 0)
                      {
                          attributeValues[i] = String.valueOf(resultSet.getBoolean(streamAttributeDtos.get(i).getAttributeName()));
                      }
                  }

                  Event event = EventConverter.eventConverter(databaseFeedConfiguration.getStreamName(),attributeValues,executionPlanDto);
                  send(databaseFeedConfiguration.getStreamName(),event);
                  if (delay>0)
                  {
                      Thread.sleep(delay);
                  }
              }
            }
        }
        catch (SQLException e)
        {
            log.error ("Error occured when generating event : " + e.getMessage());
        }
        catch (InterruptedException e)
        {
            log.error("Error occurred when sending event : " + e.getMessage());
        }
    }

}
