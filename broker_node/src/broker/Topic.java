// clase Topic
package broker;

import pubsub.Event;
import pubsub.Subscriber;

class Topic {
    private String name;

    public Topic() {
    }

    public Topic(String name) {
        this.name = name;
    }
}
