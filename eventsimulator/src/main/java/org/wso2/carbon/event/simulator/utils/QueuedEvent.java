package org.wso2.carbon.event.simulator.utils;

import org.wso2.carbon.event.executionplandelpoyer.Event;

/**
 * Created by ruwini on 2/2/17.
 */
public class QueuedEvent implements Comparable<QueuedEvent> {
    private Long id;
    private Event event;

    public QueuedEvent(Long id, Event event) {
        this.id = id;
        this.event = event;
    }

    public Event getEvent() {
        return event;
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
