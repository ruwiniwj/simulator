package org.wso2.carbon.event.simulator.utils;

/**
 * Created by ruwini on 2/2/17.
 */
import org.wso2.carbon.event.executionplandelpoyer.Event;
import org.wso2.carbon.event.simulator.databaseFeedSimulation.core.DatabaseFeedSimulator;
import scala.util.parsing.combinator.testing.Str;

import javax.swing.*;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;

public class EventSender {

    private static int minQueueSize = 0;
    private static int buffSize = 20;
    private static Queue<QueuedEvent> eventQueue = new PriorityBlockingQueue<>(buffSize, Collections.reverseOrder());
    private static boolean initialized;
    private static DatabaseFeedSimulator databaseFeedSimulator =  new DatabaseFeedSimulator();
    public static int pointer;



    public static void sendEvent(QueuedEvent event ,boolean isLast) {


        if(isLast && pointer > 0){
            pointer = pointer-1;
        }
        eventQueue.add(event);
        if (eventQueue.size() > pointer) {
            initialized = true;
            QueuedEvent output = eventQueue.poll();
            databaseFeedSimulator.send(output.getStreamName(),output.getEvent());
        }
    }

    public static void flush() {
        for (QueuedEvent event : eventQueue) {
            event = eventQueue.poll();
            databaseFeedSimulator.send(event.getStreamName(),event.getEvent());
        }
    }

    public static void setPointer(int noOfParallelSimulationSources) { pointer = noOfParallelSimulationSources;}

    public static void setMinQueueSize ( int noOfParallelSimulationSources) { minQueueSize = noOfParallelSimulationSources ;}

}
