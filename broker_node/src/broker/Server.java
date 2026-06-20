// clase estática que contacta con el Registry para dar de alta el servicio
package broker;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import pubsub.PubSub;

// no se puede instanciar ni derivar
public final class Server {
    private Server(){};
    static void init(PubSub pubsub, String port) throws RemoteException {
        //1. encontrar el registro, que se puede buscar, con LocateRegistry de registry con el puerto
        // hecho en la línea 24 de EchoServer.java
        Registry registry = LocateRegistry.getRegistry(Integer.parseInt(port));
        //2. exportar el objeto remoto, con la librería registry
        registry.rebind("nombre", pubsub);
    }
}
/*
Fase 1: Primer contacto entre un cliente y el broker
En esta fase, se plantea únicamente que un cliente pueda contactar con el broker. Para ello, hay que realizar dos acciones:

    En el método init del fichero broker_node/src/broker/Server.java hay que incluir la lógica para localizar al registry y dar de alta el servicio general.
    En el método init del fichero client_node/src/pubsubcln/Client.java hay que incluir la lógica para localizar al registry y obtener una referencia remota del servicio. 

Puede basarse en el ejemplo del servicio de eco suministrado (ficheros ejemplos_JavaRMI/1_eco/server_node/src/server/EchoServer.java y ejemplos_JavaRMI/1_eco/client_node/src/client/EchoClient.java, respectivamente), usando el nombre de servicio que considere oportuno.
Pruebas
A continuación, se muestra una prueba para comprobar la funcionalidad de este caso. Una vez arrancado el registry y el broker, tal como se explicó previamente, ejecutamos el programa de prueba (en la salida que se muestra de la ejecución del programa aparece en negrilla lo que se teclea, mientras que las respuestas se muestran subrayadas). En las pruebas que se indicarán para las distintas fases, para simplificar, se asumirá que se ejecutan en el mismo equipo, pero puede hacerlo en distintas máquinas cambiando localhost por el nombre de la máquina donde ejecuta el registry y el broker.

Para comprobar que el servicio está operativo, usaremos la operación getVersion, que ya está programada.

$ ./execute.sh Test localhost 54321
Introduzca operacion (Ctrl-D para terminar)
	getVersion|createTopic|topicList|publish|consumeEvent|initSubscriber|
	getUUID|subscriberList|subscribe|subscriberListByTopic|getEvent|
	subscribeGlob|topicListBySubscriber|unsubscribe|exit|deleteTopic
getVersion
getVersion ha devuelto: 1
 */