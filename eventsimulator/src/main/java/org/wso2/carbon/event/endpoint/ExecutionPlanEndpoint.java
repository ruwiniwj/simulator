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
import org.wso2.carbon.event.executionplandelpoyer.Exception.ExecutionPlanDeployementException;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDto;
import org.wso2.carbon.event.executionplandelpoyer.utils.ExecutionPlanParser;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;


/**
 * Simulator REST service is databaseFeedSimulation micro-service built on top of WSO2 msf4j.
 * The REST service provides the capability of deploying execution plan.
 */
@Path("/ExecutionPlan")
public class ExecutionPlanEndpoint {

    @POST
    @Path("/deploy")
    public javax.ws.rs.core.Response deployExecutionPlan(String executionPlanConfig) {
        String message;
        try {
            ExecutionPlanDto executionPlanDto = ExecutionPlanParser.executionPlanDtoParser(executionPlanConfig);
            ExecutionPlanDeployer executionPlanDeployer = ExecutionPlanDeployer.getInstance();
            executionPlanDeployer.deployExecutionPlan(executionPlanDto);
            message = "Execution PlanDeployed successfully";

        } catch (ExecutionPlanDeployementException e) {
            throw new ExecutionPlanDeployementException("Execution Plan Deployment Failed :" + e.getMessage());
        }
        String jsonString = new Gson().toJson(message);
        return Response.ok().entity(jsonString).build();
    }


}
