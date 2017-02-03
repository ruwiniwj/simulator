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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDto;
import org.wso2.carbon.event.executionplandelpoyer.StreamAttributeDto;
import org.wso2.carbon.event.executionplandelpoyer.StreamDefinitionDto;
import org.wso2.carbon.event.simulator.bean.FeedSimulationDto;
import org.wso2.carbon.event.simulator.bean.FeedSimulationStreamConfiguration;
import org.wso2.carbon.event.simulator.bean.FileStore;
import org.wso2.carbon.event.simulator.constants.EventSimulatorConstants;
import org.wso2.carbon.event.simulator.constants.RandomDataGeneratorConstants;
import org.wso2.carbon.event.simulator.csvFeedSimulation.CSVFileSimulationDto;
import org.wso2.carbon.event.simulator.csvFeedSimulation.core.FileDto;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.DatabaseFeedSimulationDto;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.util.DatabaseConnection;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.CustomBasedAttribute;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.FeedSimulationStreamAttributeDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.PrimitiveBasedAttribute;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.PropertyBasedAttributeDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.RandomDataSimulationDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.bean.RegexBasedAttributeDto;
import org.wso2.carbon.event.simulator.randomdatafeedsimulation.utils.RandomDataGenerator;
import org.wso2.carbon.event.simulator.singleventsimulator.SingleEventDto;
import scala.util.parsing.combinator.testing.Str;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.Buffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * EventSimulatorParser is an util class used to
 * convert Json string into relevant event simulation configuration object
 */
public class EventSimulatorParser {
    private static final Logger log = Logger.getLogger(EventSimulatorParser.class);

    /*
    Initialize EventSimulatorParser
     */
    private EventSimulatorParser() {
    }

    /**
     * Convert the RandomFeedSimulationString string into RandomDataSimulationDto Object
     * RandomRandomDataSimulationConfig can have one or more attribute simulation configuration.
     * these can be one of below types
     * 1.PRIMITIVEBASED : String/Integer/Float/Double/Boolean
     * 2.PROPERTYBASED  : this type indicates the type which generates meaning full data.
     * eg: If full name it generate meaning full name
     * 3.REGEXBASED     : this type indicates the type which generates data using given regex
     * 4.CUSTOMDATA     : this type indicates the type which generates data in given data list
     * <p>
     * Initialize RandomDataSimulationDto
     *
     * @param RandomFeedSimulationString RandomEventSimulationConfiguration String
     * @return RandomDataSimulationDto Object
     */
    private static RandomDataSimulationDto randomDataSimulatorParser(String RandomFeedSimulationString) {
        RandomDataSimulationDto randomDataSimulationDto = new RandomDataSimulationDto();

        JSONObject jsonObject = new JSONObject(RandomFeedSimulationString);
        ExecutionPlanDto executionPlanDto = ExecutionPlanDeployer.getInstance().getExecutionPlanDto();
        //set properties to RandomDataSimulationDto
        randomDataSimulationDto.setStreamName((String) jsonObject.get(EventSimulatorConstants.STREAM_NAME));
        if (jsonObject.getInt(EventSimulatorConstants.EVENTS) <= 0) {
            log.error("No of events to be generated can't be databaseFeedSimulation negative values");
            throw new RuntimeException("No of events to be generated can't be databaseFeedSimulation negative value");
        } else {
            randomDataSimulationDto.setEvents(jsonObject.getInt(EventSimulatorConstants.EVENTS));
        }

        randomDataSimulationDto.setDelay(jsonObject.getInt(EventSimulatorConstants.DELAY));
        StreamDefinitionDto streamDefinitionDto = executionPlanDto.getInputStreamDtoMap().get(randomDataSimulationDto.getStreamName());
        List<FeedSimulationStreamAttributeDto> feedSimulationStreamAttributeDto = new ArrayList<>();
        JSONArray jsonArray = jsonObject.getJSONArray(EventSimulatorConstants.ATTRIBUTE_CONFIGURATION);
        if (jsonArray.length() != streamDefinitionDto.getStreamAttributeDtos().size()) {
            throw new EventSimulationException("Configuration of attribute for " +
                    "feed simulation is missing in " + streamDefinitionDto.getStreamName() +
                    " : " + " No of attribute in stream " + streamDefinitionDto.getStreamAttributeDtos().size());
        }

        //convert each attribute simulation configuration as relevant objects

        Gson gson = new Gson();
        for (int i = 0; i < jsonArray.length(); i++) {
            if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.RANDOMDATAGENERATORTYPE)) {
                if (jsonArray.getJSONObject(i).getString(EventSimulatorConstants.RANDOMDATAGENERATORTYPE).
                        compareTo(RandomDataGeneratorConstants.PROPERTY_BASED_ATTRIBUTE) == 0) {
                    if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.PROPERTYBASEDATTRIBUTE_CATEGORY)
                            && !jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.PROPERTYBASEDATTRIBUTE_PROPERTY)) {
                        PropertyBasedAttributeDto propertyBasedAttributeDto =
                                gson.fromJson(String.valueOf(jsonArray.getJSONObject(i)), PropertyBasedAttributeDto.class);
                        feedSimulationStreamAttributeDto.add(propertyBasedAttributeDto);
                    } else {
                        log.error("Category and property should not be null value for " +
                                RandomDataGeneratorConstants.PROPERTY_BASED_ATTRIBUTE
                                + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName());
                        throw new EventSimulationException("Category and property should not be null value for " +
                                RandomDataGeneratorConstants.PROPERTY_BASED_ATTRIBUTE
                                + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName());
                    }
                } else if (jsonArray.getJSONObject(i).getString(EventSimulatorConstants.RANDOMDATAGENERATORTYPE).
                        compareTo(RandomDataGeneratorConstants.REGEX_BASED_ATTRIBUTE) == 0) {
                    if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.REGEXBASEDATTRIBUTE_PATTERN)) {
                        RegexBasedAttributeDto regexBasedAttributeDto =
                                gson.fromJson(String.valueOf(jsonArray.getJSONObject(i)), RegexBasedAttributeDto.class);
                        RandomDataGenerator.validateRegularExpression(regexBasedAttributeDto.getPattern());
                        feedSimulationStreamAttributeDto.add(regexBasedAttributeDto);
                    } else {
                        log.error("Pattern should not be null value for " + RandomDataGeneratorConstants.REGEX_BASED_ATTRIBUTE
                                + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName());
                        throw new EventSimulationException("Pattern should not be null value for " +
                                RandomDataGeneratorConstants.REGEX_BASED_ATTRIBUTE
                                + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName());
                    }
                } else if (jsonArray.getJSONObject(i).getString(EventSimulatorConstants.RANDOMDATAGENERATORTYPE).
                        compareTo(RandomDataGeneratorConstants.PRIMITIVE_BASED_ATTRIBUTE) == 0) {
                    if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.PRIMITIVEBASEDATTRIBUTE_MIN)
                            && !jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.PRIMITIVEBASEDATTRIBUTE_MAX)
                            && !jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.PRIMITIVEBASEDATTRIBUTE_LENGTH_DECIMAL)) {
                        PrimitiveBasedAttribute primitiveBasedAttribute =
                                gson.fromJson(String.valueOf(jsonArray.getJSONObject(i)), PrimitiveBasedAttribute.class);
                        feedSimulationStreamAttributeDto.add(primitiveBasedAttribute);
                    } else {
                        log.error("Min,Max and Length value should not be null value for " +
                                RandomDataGeneratorConstants.PRIMITIVE_BASED_ATTRIBUTE +
                                streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName());
                        throw new EventSimulationException("Min,Max and Length value should not be null value for " +
                                RandomDataGeneratorConstants.PRIMITIVE_BASED_ATTRIBUTE +
                                streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName());
                    }
                } else if (jsonArray.getJSONObject(i).getString(EventSimulatorConstants.RANDOMDATAGENERATORTYPE).
                        compareTo(RandomDataGeneratorConstants.CUSTOM_DATA_BASED_ATTRIBUTE) == 0) {
                    if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.CUSTOMDATABASEDATTRIBUTE_LIST)) {
                        CustomBasedAttribute customBasedAttribute = new CustomBasedAttribute();
                        customBasedAttribute.setType(jsonArray.getJSONObject(i).getString(EventSimulatorConstants.RANDOMDATAGENERATORTYPE));
                        customBasedAttribute.setCustomData(jsonArray.getJSONObject(i).getString(EventSimulatorConstants.CUSTOMDATABASEDATTRIBUTE_LIST));
                        feedSimulationStreamAttributeDto.add(customBasedAttribute);
                    } else {
                        // TODO: 21/12/16 only throw put stream name
                        log.error("Data list is not given for "
                                + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName()
                                + RandomDataGeneratorConstants.CUSTOM_DATA_BASED_ATTRIBUTE);
                        throw new EventSimulationException("Data list is not given for "
                                + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName()
                                + RandomDataGeneratorConstants.CUSTOM_DATA_BASED_ATTRIBUTE);
                    }
                }
            } else {
                log.error("Random Data Generator option is required  for an attribute" +
                        streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName() + " : " +
                        RandomDataGeneratorConstants.PROPERTY_BASED_ATTRIBUTE + "/" +
                        RandomDataGeneratorConstants.REGEX_BASED_ATTRIBUTE + "/" + RandomDataGeneratorConstants.PRIMITIVE_BASED_ATTRIBUTE +
                        "/" + RandomDataGeneratorConstants.CUSTOM_DATA_BASED_ATTRIBUTE);
                throw new EventSimulationException("Random Data Generator option is required  for an attribute"
                        + streamDefinitionDto.getStreamAttributeDtos().get(i).getAttributeName() + " : " +
                        RandomDataGeneratorConstants.PROPERTY_BASED_ATTRIBUTE + "/" +
                        RandomDataGeneratorConstants.REGEX_BASED_ATTRIBUTE + "/" + RandomDataGeneratorConstants.PRIMITIVE_BASED_ATTRIBUTE +
                        "/" + RandomDataGeneratorConstants.CUSTOM_DATA_BASED_ATTRIBUTE);
            }
        }
        randomDataSimulationDto.setFeedSimulationStreamAttributeDto(feedSimulationStreamAttributeDto);

        return randomDataSimulationDto;
    }


    /**
     * Convert the singleEventSimulationConfigurationString string into SingleEventDto Object
     * Initialize SingleEventDto
     *
     * @param singleEventSimulationConfigurationString singleEventSimulationConfigurationString String
     * @return SingleEventDto Object
     */
    public static SingleEventDto singleEventSimulatorParser(String singleEventSimulationConfigurationString) {
        SingleEventDto singleEventDto;
        ObjectMapper mapper = new ObjectMapper();
        //Convert the singleEventSimulationConfigurationString string into SingleEventDto Object
        try {
            singleEventDto = mapper.readValue(singleEventSimulationConfigurationString, SingleEventDto.class);
            ExecutionPlanDto executionPlanDto = ExecutionPlanDeployer.getInstance().getExecutionPlanDto();
            StreamDefinitionDto streamDefinitionDto = executionPlanDto.getInputStreamDtoMap().get(singleEventDto.getStreamName());
            if (singleEventDto.getAttributeValues().size() != streamDefinitionDto.getStreamAttributeDtos().size()) {
                log.error("No of Attribute values is not equal to attribute size in " +
                        singleEventDto.getStreamName() + " : Required attribute size " +
                        streamDefinitionDto.getStreamAttributeDtos().size());
                throw new EventSimulationException("No of Attribute value is not equal to attribute size in Input Stream : No of Attributes in " +
                        singleEventDto.getStreamName() + " is " + streamDefinitionDto.getStreamAttributeDtos().size());
            }
        } catch (IOException e) {
            log.error("Exception occurred when parsing json to Object ");
            throw new EventSimulationException("Exception occurred when parsing json to Object " + e.getMessage());
        }
        return singleEventDto;
    }

    /**
     * Convert the csvFileDetail string into CSVFileSimulationDto Object
     * <p>
     * Initialize CSVFileSimulationDto
     * Initialize FileStore
     *
     * @param csvFileDetail csvFileDetail String
     * @return CSVFileSimulationDto Object
     */
    private static CSVFileSimulationDto fileFeedSimulatorParser(String csvFileDetail) {
        CSVFileSimulationDto csvFileSimulationDto = new CSVFileSimulationDto();
        FileStore fileStore = FileStore.getFileStore();

        JSONObject jsonObject = new JSONObject(csvFileDetail);
        csvFileSimulationDto.setStreamName((String) jsonObject.get(EventSimulatorConstants.STREAM_NAME));
        csvFileSimulationDto.setFileName((String) jsonObject.get(EventSimulatorConstants.FILE_NAME));
        //get the fileDto from FileStore if file exist and set this value.
        FileDto fileDto;
        System.out.println(fileStore.getFileInfoMap());
        try {
            if (fileStore.checkExists(csvFileSimulationDto.getFileName())) {
                fileDto = fileStore.getFileInfoMap().get(csvFileSimulationDto.getFileName());
                System.out.println(fileDto);
            } else {
                // TODO: 21/12/16 file name is not provided
                log.error("File does not Exist : " + csvFileSimulationDto.getFileName());
                throw new EventSimulationException("File does not Exist");
            }
            csvFileSimulationDto.setFileDto(fileDto);
            csvFileSimulationDto.setDelimiter((String) jsonObject.get(EventSimulatorConstants.DELIMITER));
            csvFileSimulationDto.setDelay(jsonObject.getInt(EventSimulatorConstants.DELAY));


        }catch (Exception FileNotFound)
        {
            System.out.println("File not found : " +FileNotFound.getMessage());
        }
        return csvFileSimulationDto;
    }
    // TODO R database parser
    /**
     * Convert the database configuration file into a DatabaseFeedSimulationDto object
     *
     * @param databaseConfigurations : database configuration string
     * @return a DatabaseFeedSimulationDto object
     * */

    private static DatabaseFeedSimulationDto databaseFeedSimulationParser(String databaseConfigurations){

       DatabaseFeedSimulationDto databaseFeedSimulationDto = new DatabaseFeedSimulationDto();
       JSONObject jsonObject= new JSONObject(databaseConfigurations);
       ExecutionPlanDto executionPlanDto = ExecutionPlanDeployer.getInstance().getExecutionPlanDto();

//       assign values for database configuration attributes
       databaseFeedSimulationDto.setDatabaseConfigName(jsonObject.getString(EventSimulatorConstants.DATABASE_CONFIGURATION_NAME));
       databaseFeedSimulationDto.setDatabaseName(jsonObject.getString(EventSimulatorConstants.DATABASE_NAME));
       databaseFeedSimulationDto.setUsername(jsonObject.getString(EventSimulatorConstants.USER_NAME));
       databaseFeedSimulationDto.setPassword(jsonObject.getString(EventSimulatorConstants.PASSWORD));
       databaseFeedSimulationDto.setTableName(jsonObject.getString(EventSimulatorConstants.TABLE_NAME));
       JSONArray jsonArray = jsonObject.getJSONArray(EventSimulatorConstants.COLUMN_NAMES_AND_TYPES);
       databaseFeedSimulationDto.setStreamName(jsonObject.getString(EventSimulatorConstants.STREAM_NAME));
       databaseFeedSimulationDto.setDelay(jsonObject.getInt(EventSimulatorConstants.DELAY));

       Gson gson = new Gson();
       HashMap<String,String> columnAndTypes = new HashMap<>();

//       insert the specified column names and types into a hashmap and insert it to database configuration
       for (int i=0; i<jsonArray.length();i++) {
           if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.COLUMN_NAME)&&
                   !jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.COLUMN_TYPE)) {

               columnAndTypes.put(jsonArray.getJSONObject(i).getString(EventSimulatorConstants.COLUMN_NAME),
                       jsonArray.getJSONObject(i).getString(EventSimulatorConstants.COLUMN_TYPE));
           } else {
               throw new EventSimulationException("Column name and type cannot contain null values");
           }
       }

       databaseFeedSimulationDto.setColumnNamesAndTypes(columnAndTypes);

       return databaseFeedSimulationDto;
    }


    /**
     * Convert the feedSimulationDetails string into FeedSimulationDto Object
     * Three types of feed simulation are applicable for an input stream
     * These types are
     * 1. CSV file feed simulation : Simulate using CSV File
     * 2. Database Simulation : Simulate using Database source
     * 3. Random data simulation : Simulate using Generated random Data
     * <p>
     * Initialize FeedSimulationDto
     *
     * @param feedSimulationDetails feedSimulationDetails
     * @return FeedSimulationDto Object
     */
    public static FeedSimulationDto feedSimulationParser(String feedSimulationDetails) {

        FeedSimulationDto feedSimulationDto = new FeedSimulationDto();
        JSONObject jsonObject = new JSONObject(feedSimulationDetails);

        List<FeedSimulationStreamConfiguration> feedSimulationStreamConfigurationList = new ArrayList<>();

        JSONArray jsonArray = jsonObject.getJSONArray(EventSimulatorConstants.FEED_SIMULATION_STREAM_CONFIGURATION);

        if (jsonObject.getBoolean(EventSimulatorConstants.ORDER_BY_TIMESTAMP)) {
            feedSimulationDto.setOrderByTimeStamp(jsonObject.getBoolean(EventSimulatorConstants.ORDER_BY_TIMESTAMP));
            feedSimulationDto.setNoOfParallelSimulationSources(jsonArray.length());
            EventSender.setPointer(feedSimulationDto.getNoOfParallelSimulationSources());
            EventSender.setMinQueueSize(feedSimulationDto.getNoOfParallelSimulationSources());
        }

        //check the simulation type for databaseFeedSimulation given stream and convert the string to relevant configuration object
        //            1. CSV file feed simulation : Simulate using CSV File
        //            2. Database Simulation : Simulate using Database source
        //            3. Random data simulation : Simulate using Generated random Data

        for (int i = 0; i < jsonArray.length(); i++) {
            if (!jsonArray.getJSONObject(i).isNull(EventSimulatorConstants.FEED_SIMULATION_TYPE)) {
                String feedSimulationType = jsonArray.getJSONObject(i).getString(EventSimulatorConstants.FEED_SIMULATION_TYPE);

                switch (feedSimulationType) {
                    case EventSimulatorConstants.RANDOM_DATA_SIMULATION:
                        RandomDataSimulationDto randomDataSimulationDto =
                                randomDataSimulatorParser(String.valueOf(jsonArray.getJSONObject(i)));
                        randomDataSimulationDto.setSimulationType(EventSimulatorConstants.RANDOM_DATA_SIMULATION);
                        feedSimulationStreamConfigurationList.add(randomDataSimulationDto);
                        break;
                    case EventSimulatorConstants.FILE_FEED_SIMULATION:
                        CSVFileSimulationDto csvFileConfig = EventSimulatorParser.
                                fileFeedSimulatorParser(String.valueOf(jsonArray.getJSONObject(i)));
                        csvFileConfig.setSimulationType(EventSimulatorConstants.FILE_FEED_SIMULATION);
                        //streamConfigurationListMap.put(csvFileConfig.getStreamName(),csvFileConfig);
                        feedSimulationStreamConfigurationList.add(csvFileConfig);
                        break;
                    // TODO: 20/12/16 database

                    case EventSimulatorConstants.DATABASE_FEED_SIMULATION:
                        DatabaseFeedSimulationDto databaseFeedSimulationDto =
                                EventSimulatorParser.databaseFeedSimulationParser(String.valueOf(jsonArray.getJSONObject(i)));
                        if (feedSimulationDto.getOrderByTimeStamp()) {
                            databaseFeedSimulationDto.setTimestampAttribute(String.valueOf(jsonArray.getJSONObject(i).
                                    getString(EventSimulatorConstants.TIMESTAMP_ATTRIBUTE)));
                        }
                        databaseFeedSimulationDto.setSimulationType(EventSimulatorConstants.DATABASE_FEED_SIMULATION);
                        feedSimulationStreamConfigurationList.add(databaseFeedSimulationDto);

                        break;
                    default:
                        log.error(feedSimulationType + "is not available , required only : " +
                                EventSimulatorConstants.RANDOM_DATA_SIMULATION
                                + " or" + EventSimulatorConstants.FILE_FEED_SIMULATION + " or" +
                                EventSimulatorConstants.DATABASE_FEED_SIMULATION);
                        throw new EventSimulationException(feedSimulationType + "is not available , required only : " +
                                EventSimulatorConstants.RANDOM_DATA_SIMULATION
                                + " or" + EventSimulatorConstants.FILE_FEED_SIMULATION + " or" +
                                EventSimulatorConstants.DATABASE_FEED_SIMULATION);
                }
            } else {
                log.error(EventSimulatorConstants.FEED_SIMULATION_TYPE + "is null");
                throw new EventSimulationException(EventSimulatorConstants.FEED_SIMULATION_TYPE + "is null : required"
                        + EventSimulatorConstants.RANDOM_DATA_SIMULATION
                        + " or" + EventSimulatorConstants.FILE_FEED_SIMULATION + " or" +
                        EventSimulatorConstants.DATABASE_FEED_SIMULATION);
            }
        }
        feedSimulationDto.setStreamConfigurationList(feedSimulationStreamConfigurationList);


        return feedSimulationDto;
    }

}
