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

class SubscriberImpl extends UnicastRemoteObject implements Subscriber  {
    public static final long serialVersionUID=1234567890L;
    UUID subUUID; // para facilitar depuración
    PubSubImpl ps; // para acceder a funcionalidad del servicio general
    // para notificar al subscriptor de creación y destrucción de temas
    transient SubscriberCallback scbk; 
    private List<String> topicsSubscribed = new java.util.ArrayList<>();

    public SubscriberImpl(PubSubImpl p, SubscriberCallback s) throws RemoteException {
        super(); // extiende UnicastRemoteObject(); 
        scbk=s;
        subUUID = UUID.randomUUID();
	ps=p;
    }
    public UUID getUUID() throws RemoteException {
        return subUUID;
    }

   
    public int subscribe(String topic, boolean glob) throws RemoteException {
        if(glob==false){
            Topic t = ps.getTopic(topic);
            if(t!=null&&!topicsSubscribed.contains(topic)){
                topicsSubscribed.add(topic);
                t.addSubscriber(this);
                return 1;
            }
            return 0;
        }
        return 0;
    }
    public Event getEvent() throws RemoteException {
        return null;
    }
    public Collection<String> topicListBySubscriber() throws RemoteException {
        return new ArrayList<>(topicsSubscribed);
    }
    public boolean unsubscribe(String topic) throws RemoteException {
        return true;
    }
    public void exit() throws RemoteException {
    }
}
