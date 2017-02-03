package org.wso2.carbon.event.simulator.utils;

import org.wso2.carbon.event.executionplandelpoyer.Event;

/**
 * Created by ruwini on 2/2/17.
 */
public class QueuedEvent implements Comparable<QueuedEvent> {
    private int id;
    private Event event;
    private String streamName;

    public Event getEvent() { return event;}

    public String getStreamName() { return streamName;}

    public QueuedEvent(int id, Event event, String streamName) {
        this.id = id;
        this.event = event;
        this.streamName = streamName;
    }

    public int compareTo(QueuedEvent event) {
        if (id < event.id) {
            return 1;
        } else if (id > event.id) {
            return -1;
        } else {
            return 0;
        }
    }

}
