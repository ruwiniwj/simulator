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

package org.wso2.carbon.event.endpoint;


import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.wso2.carbon.event.simulator.bean.FeedSimulationDto;
import org.wso2.carbon.event.simulator.csvFeedSimulation.core.FileUploader;
import org.wso2.carbon.event.simulator.exception.EventSimulationException;
import org.wso2.carbon.event.simulator.exception.ValidationFailedException;
import org.wso2.carbon.event.simulator.singleventsimulator.SingleEventDto;
import org.wso2.carbon.event.simulator.utils.EventSimulatorParser;
import org.wso2.carbon.event.simulator.utils.EventSimulatorServiceExecutor;
import org.wso2.msf4j.formparam.FileInfo;
import org.wso2.msf4j.formparam.FormDataParam;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;


/**
 * Simulator REST service is databaseFeedSimulation micro-service built on top of WSO2 msf4j.
 * The REST service provides the capability of simulating events.
 */

@Path("/EventSimulation")
public class EventSimulatorRestService {
    private static final Logger log = Logger.getLogger(EventSimulatorRestService.class);

    /**
     * Event simulator service executor for event simulator REST service.
     */
    private EventSimulatorServiceExecutor eventSimulatorServiceExecutor;

    /**
     * Initializes the service classes for resources.
     */
    //// TODO: 19/12/16 shutdown execution plan
    public EventSimulatorRestService() {
        eventSimulatorServiceExecutor = new EventSimulatorServiceExecutor();
    }

    /**
     * Send single event for simulation
     *
     * @param simulationString jsonString to be converted to SingleEventDto object from the request Json body.
     *                         <p>
     *                         http://localhost:8080/EventSimulation/singleEventSimulation
     *                         <pre>
     *                         curl  -X POST -d '{"streamName":"cseEventStream","attributeValues":["WSO2","345","56"]}' http://localhost:8080/EventSimulation/singleEventSimulation
     *                         </pre>
     *                         <p>
     *                         Eg :simulationString: {
     *                         "streamName":"cseEventStream",
     *                         "attributeValues":attributeValue
     *                         };
     */
    @POST
    @Path("/singleEventSimulation")
    public Response singleEventSimulation(String simulationString) {
        if (log.isDebugEnabled()) {
            log.debug("Single Event Simulation");
        }
        String jsonString;

        try {
            //parse json string to SingleEventDto object
            SingleEventDto singleEventSimulationConfiguration = EventSimulatorParser.singleEventSimulatorParser(simulationString);

            //start single event simulation
            eventSimulatorServiceExecutor.simulateSingleEvent(singleEventSimulationConfiguration);
            jsonString = new Gson().toJson("Event is send successfully");
        } catch (EventSimulationException e) {
            throw new EventSimulationException("Single Event simulation failed : " + e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }

    /**
     * Deploy CSV filereturn Response.ok().entity("File uploaded").build();
     * <p>
     * This function use FormDataParam annotation. WSO@2 MSF4J supports this annotation and multipart/form-data content type.
     * <p>
     * </p>
     * The FormDataParam annotation supports complex types and collections (such as List, Set and SortedSet),
     * with the multipart/form-data content type and supports files along with form field submissions.
     * It supports directly to get the file objects in databaseFeedSimulation file upload by using the @FormDataParam  annotation.
     * This annotation can be used with all FormParam supported data types plus file and bean types as well as InputStreams.
     * </p>
     *
     * @param fileInfo        FileInfo bean to hold the filename and the content type attributes of the particular InputStream
     * @param fileInputStream InputStream of the file
     * @return Response of completion of process
     * <p>
     * http://localhost:8080/EventSimulation/fileUpload
     */
    @POST
    @Path("/fileUpload")
    @Consumes(MediaType.MULTIPART_FORM_DATA)

    public Response uploadFile(@FormDataParam("file") FileInfo fileInfo,
                               @FormDataParam("file") InputStream fileInputStream) {
        String jsonString;
        /*
        Get singleton instance of FileUploader
         */

        FileUploader fileUploader = FileUploader.getFileUploaderInstance();
        try {
            fileUploader.uploadFile(fileInfo, fileInputStream);
            jsonString = new Gson().toJson("File is uploaded");
        } catch (ValidationFailedException | EventSimulationException e) {
            throw new EventSimulationException("Failed file upload : " + e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }

    /**
     * Delete the file
     * <p>
     * This function use FormDataParam annotation. WSO@2 MSF4J supports this annotation and multipart/form-data content type.
     * <p>
     *
     * @param fileName File Name
     * @return Response of completion of process
     * <p>
     * http://localhost:8080/EventSimulation/deleteFile
     */
    @POST
    @Path("/deleteFile")
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response deleteFile(@FormDataParam("fileName") String fileName) {
        String jsonString;
        /*
         * Get singleton instance of FileUploader
         */
        FileUploader fileUploader = FileUploader.getFileUploaderInstance();
        try {
            fileUploader.deleteFile(fileName);
            jsonString = new Gson().toJson("File is deleted");
        } catch (EventSimulationException e) {
            throw new EventSimulationException("Failed file delete : " + e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }


    /**
     * This method produces service for feed simulation
     * <p>
     * For an execution plan It may have one or more input streams.
     * this method provides the capability to simulate each input streams in different cases.
     * such as simulate using CSV File, simulate using Random Data and simulate using
     * database resource.
     * </p>
     *
     * @param feedSimulationConfigDetails jsonString to be converted to FeedSimulationDto object from the request Json body.
     * @return Response of completion of process
     * <p>
     * <pre>
     *     curl  -X POST -d '{"orderByTimeStamp" : "false","streamConfiguration"
     *     :[{"simulationType" : "RandomDataSimulation","streamName": "cseEventStream2",
     *     "events": "20","delay": "1000","attributeConfiguration":[{"type": "CUSTOMDATA",
     *     "list": "WSO2,IBM"},{"type": "REGEXBASED","pattern": "[+]?[0-9]*\\.?[0-9]+"},
     *     {"type": "PRIMITIVEBASED","min": "2","max": "200","length": "2",}]},
     *     {"simulationType" : "FileFeedSimulation","streamName" : "cseEventStream","fileName"   : "cseteststream.csv",
     *     "delimiter"  : ",","delay": "1000"}]}' http://localhost:8080/EventSimulation/feedSimulation
     * </pre>
     * <p>
     * http://localhost:8080/EventSimulation/feedSimulation
     */
    @POST
    @Path("/feedSimulation")
    public Response feedSimulation(String feedSimulationConfigDetails) {
        String jsonString;
        try {
            //parse json string to FeedSimulationDto object
            FeedSimulationDto feedSimulationConfig =EventSimulatorParser.feedSimulationParser(feedSimulationConfigDetails);
            //start feed simulation
            eventSimulatorServiceExecutor.simulateFeedSimulation(feedSimulationConfig);
            jsonString = new Gson().toJson("Feed simulation starts successfully");
        } catch (EventSimulationException e) {
            throw new EventSimulationException(e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }

    /**
     * Stop the simulation process
     *
     * @return Response of completion of process
     * <p>
     * http://localhost:8080/EventSimulation/feedSimulation/stop
     */
    @POST
    @Path("/feedSimulation/stop")
    public Response stop() throws InterruptedException {
        String jsonString;
        //stop feed simulation
        try {
            eventSimulatorServiceExecutor.stop();
            jsonString = new Gson().toJson("Feed simulation is stopped");
        } catch (EventSimulationException e) {
            throw new EventSimulationException(e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }

    /**
     * pause the simulation process
     *
     * @return Response of completion of process
     * @throws InterruptedException Interrupted Exception
     *                              <p>
     *                              http://localhost:8080/EventSimulation/feedSimulation/pause
     */
    @POST
    @Path("/feedSimulation/pause")
    public Response pause() throws InterruptedException {
        String jsonString;
        //pause feed simulation
        try {
            eventSimulatorServiceExecutor.pause();
            jsonString = new Gson().toJson("Feed simulation is paused");
        } catch (EventSimulationException e) {
            throw new EventSimulationException(e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }

    /**
     * resume the simulation
     *
     * @return Response of completion of process
     * @throws InterruptedException Interrupted Exception
     *                              <p>
     *                              http://localhost:8080/EventSimulation/feedSimulation/resume
     */
    @POST
    @Path("/feedSimulation/resume")
    public Response resume() throws InterruptedException {
        String jsonString;
        //pause feed simulation
        try {
            eventSimulatorServiceExecutor.resume();
            jsonString = new Gson().toJson("success");
        } catch (EventSimulationException e) {
            throw new EventSimulationException(e.getMessage());
        }
        return Response.ok().entity(jsonString).build();
    }


}
