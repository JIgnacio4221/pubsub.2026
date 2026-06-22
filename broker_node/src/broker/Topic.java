// clase Topic
package broker;

import pubsub.Event;
import pubsub.Subscriber;

import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;

class Topic {
    private String name;
    private Queue<Event> events = new LinkedList<>();
    private List<SubscriberImpl> subscribers = new ArrayList<>();

    public Topic(String name) {
        this.name = name;
    }

    public void addEvent(Event event) {
        events.add(event);
    }

    public Event consumeEvent() {
        return events.poll();
    }

    public void addSubscriber(SubscriberImpl subscriber) {
        if (!subscribers.contains(subscriber)) {
            subscribers.add(subscriber);
        }
    }

    public List<SubscriberImpl> getSubscribers() {
        return subscribers;
    }
}
