// clase Topic
package broker;

import pubsub.Event;
import pubsub.Subscriber;

import java.util.Queue;
import java.util.LinkedList;

class Topic {
    private String name;
    private Queue<Event> events = new LinkedList<>();

    public Topic(String name) {
        this.name = name;
    }

    public void addEvent(Event event) {
        events.add(event);
    }

    public Event consumeEvent() {
        return events.poll();
    }
}
