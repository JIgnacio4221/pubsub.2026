// Servidor que implementa la interfaz remota PubSub
package broker;

import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import pubsub.Event;
import pubsub.PubSub;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

class PubSubImpl extends UnicastRemoteObject implements PubSub {
    public static final long serialVersionUID = 1234567890L;
    private Map<String, Topic> topics = new HashMap<>();
    private List<SubscriberImpl> subscribers = new ArrayList<>();

    public PubSubImpl() throws RemoteException {
    }

    public int getVersion() throws RemoteException { // ya programada
        return version;
    }

    public synchronized boolean createTopic(String topic) throws RemoteException {
        if (!topics.containsKey(topic)) {
            topics.put(topic, new Topic(topic));
            for (int i = 0; i < subscribers.size(); i++) {
                // recorrer la lista de suscriptores, y para cada uno que
                // tenga callback no nulo, llamar a callback.topicAdded(topic)
                SubscriberImpl subscriber = subscribers.get(i);
                // el callback es scbk de la clase SubscriberImpl
                if (subscriber.scbk != null) {
                    subscriber.scbk.topicAdded(topic);
                }
            }
            return true;
        }
        return false;
    }

    Topic getTopic(String name) {
        return topics.get(name);
    }

    public synchronized Collection<String> topicList() throws RemoteException {
        return new ArrayList<>(topics.keySet());
    }

    public synchronized boolean publish(Event ev) throws RemoteException {
        Topic topic = topics.get(ev.getTopic());
        if (topic != null) {
            topic.addEvent(ev);
            for (SubscriberImpl sub : topic.getSubscribers()) {
                sub.addEvent(ev);
            }
            return true;
        }
        return false;
    }

    public synchronized Event consumeEvent(String topic) throws RemoteException {
        Topic t = topics.get(topic);
        if (t == null)
            throw new NoSuchObjectException("Topic no existe");
        return t.consumeEvent();
    }

    public synchronized Subscriber initSubscriber(SubscriberCallback c) throws RemoteException {
        // es crear el usuarioSuscrito, añadirlo a la lista y retornarlo
        SubscriberImpl subscriber = new SubscriberImpl(this, c);
        subscribers.add(subscriber);
        return subscriber;
    }

    public synchronized Collection<Subscriber> subscriberList() throws RemoteException {
        // simplemente es retornar la lista
        return new ArrayList<>(subscribers);
    }

    public synchronized Collection<Subscriber> subscriberListByTopic(String topic) throws RemoteException {
        Topic t = topics.get(topic);
        if (t == null) return null;
        return new ArrayList<>(t.getSubscribers());
    }

    public synchronized boolean deleteTopic(String topic) throws RemoteException {
        return false;
    }

    static public void main(String args[]) {
        if (args.length != 1) {
            System.err.println("Usage: PubSubImpl registryPortNumber");
            return;
        }
        try {
            PubSub ps = new PubSubImpl();
            Server.init(ps, args[0]);
        } catch (Exception e) {
            System.err.println("PubSubImpl exception: " + e.toString());
            System.exit(1);
        }
    }
}