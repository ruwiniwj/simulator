package org.wso2.carbon.event.simulator.utils;

/**
 * Created by ruwini on 2/2/17.
 */

import org.apache.log4j.Logger;
import org.wso2.carbon.event.executionplandelpoyer.Event;
import org.wso2.carbon.event.executionplandelpoyer.ExecutionPlanDeployer;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

// TODO: 2/4/17 Are we sorting per configuration or per stream? For now its per stream
public class EventSender {
    private static final Logger log = Logger.getLogger(EventSender.class);
    private static final EventSender instance = new EventSender();
    private final Map<String, AtomicInteger> taskCounter = new ConcurrentHashMap<>();
    private int minQueueSize = 3;
    private int queueLength = 20;
    private Map<String, Queue<QueuedEvent>> eventQueue = new ConcurrentHashMap<>();

    private EventSender() {
    }

    public static EventSender getInstance() {
        return instance;
    }

    public void addSimulator(String streamName) {
        synchronized (taskCounter) {
            AtomicInteger counter;
            if (!taskCounter.containsKey(streamName)) {
                counter = new AtomicInteger(0);
                taskCounter.put(streamName, counter);
            } else {
                counter = taskCounter.get(streamName);
            }
            counter.incrementAndGet();
        }
    }

    public void removeSimulator(String streamName) {
        synchronized (taskCounter) {
            if (taskCounter.get(streamName).decrementAndGet() == 0) {
                flush(streamName);
                taskCounter.remove(streamName);
            }
        }
    }

    public void sendEvent(QueuedEvent event) {
        synchronized (this) {
            Queue<QueuedEvent> queue = eventQueue.get(event.getEvent().getStreamName());
            if (queue == null) {
                queue = new PriorityBlockingQueue<>(queueLength, Collections.reverseOrder());
                eventQueue.putIfAbsent(event.getEvent().getStreamName(), queue);
            }
            queue.add(event);
            if (queue.size() > minQueueSize) {
                sendEvent(queue.poll().getEvent());
            }
        }
    }

    public void sendEvent(Event event) {
        try {
            // get the input handler for particular input stream Name
            // and send the event to that input handler
            ExecutionPlanDeployer
                    .getInstance()
                    .getInputHandlerMap()
                    .get(event.getStreamName())
                    .send(event.getEventData());
        } catch (InterruptedException e) {
            log.error("Error occurred when sending event :" + e.getMessage());
        }
    }

    public void flush(String streamName) {
        synchronized (this) {
            Queue<QueuedEvent> queue = eventQueue.get(streamName);
            if (queue != null) {
                for (QueuedEvent event : queue) {
                    sendEvent(queue.poll().getEvent());
                }
            }
        }
    }

    public void setMinQueueSize(int minQueueSize) {
        this.minQueueSize = minQueueSize;
    }

    public void setQueueLength(int queueLength) {
        this.queueLength = queueLength;
    }
}

