// Clase que implementa la interfaz remota Subscriber
package broker;

import java.rmi.RemoteException;
import java.rmi.NoSuchObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.UUID;
import pubsub.Subscriber;
import pubsub.SubscriberCallback;
import pubsub.Event;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

class SubscriberImpl extends UnicastRemoteObject implements Subscriber {
    public static final long serialVersionUID = 1234567890L;
    UUID subUUID; // para facilitar depuración
    PubSubImpl ps; // para acceder a funcionalidad del servicio general
    // para notificar al subscriptor de creación y destrucción de temas
    transient SubscriberCallback scbk;
    private List<String> topicsSubscribed = new ArrayList<>();
    private LinkedList<Event> events = new LinkedList<>();
    private boolean finished = false;
    
    public SubscriberImpl(PubSubImpl p, SubscriberCallback s) throws RemoteException {
        super(); // extiende UnicastRemoteObject();
        scbk = s;
        subUUID = UUID.randomUUID();
        ps = p;
    }

    public UUID getUUID() throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        return subUUID;
    }

    public int subscribe(String topic, boolean glob) throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        int contadorSuscripciones = 0;
        if (glob == false) {
            Topic t = ps.getTopic(topic);
            if (t != null && !topicsSubscribed.contains(topic)) {
                topicsSubscribed.add(topic);
                t.addSubscriber(this);
                contadorSuscripciones++;
            }
        } else {
            // en lugar de suscribirse a un tema concreto,
            // el patron se compara contra todos
            // los temas existentes y el suscriptor se suscribe a todos los quee encajan
            PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:" + topic);

            for (String nombreTema : ps.topicList()) {
                if (matcher.matches(Paths.get(nombreTema))) {
                    Topic t = ps.getTopic(nombreTema);
                    if (t != null && !topicsSubscribed.contains(nombreTema)) {
                        topicsSubscribed.add(nombreTema);
                        t.addSubscriber(this);
                        contadorSuscripciones++;
                    }
                }
            }
        }
        return contadorSuscripciones;
    }

    public Event getEvent() throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        if (!events.isEmpty()) {
            return events.poll();
        }
        return null;
    }

    public void addEvent(Event event) throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        events.add(event);
    }

    public Collection<String> topicListBySubscriber() throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        return new ArrayList<>(topicsSubscribed);
    }

    public boolean unsubscribe(String topic) throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        if (topicsSubscribed.contains(topic)) {
            topicsSubscribed.remove(topic);
            Topic t = ps.getTopic(topic);
            if (t != null) {
                t.getSubscribers().remove(this);
            }
            return true;
        }
        return false;
    }

    public void exit() throws RemoteException {
        if (finished) throw new NoSuchObjectException("this subscriber has already finished");
        for (String topic : new ArrayList<>(topicsSubscribed)) {
            unsubscribe(topic);
        }
        ps.removeSubscriber(this);
        finished = true;
        
    }
}
