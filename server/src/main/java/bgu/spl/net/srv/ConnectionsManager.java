package bgu.spl.net.srv;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionsManager<T> implements Connections<T> {

    /*
    Class that helps saving and maintaining all active connections.
    Maps a connection handler to a connection ID.
    Manages subscription, via subscription manager object.
    
    */
    private ConcurrentHashMap<Integer,ConnectionHandler<T>> connecetions;
    private SubscriptionManager subManager;
    private AtomicInteger idGen;
   
    
    /*
    Connection manager constructor, constructs the above fields.
    */
    public ConnectionsManager(){
        this.connecetions = new ConcurrentHashMap<>();
        this.subManager = new SubscriptionManager();
        this.idGen = new AtomicInteger(0);
    }

    /*
    Method that handles registering new connection.
    */
    public void connect(ConnectionHandler<T> newClient,int conId)
    {
        this.connecetions.put(conId,newClient); // insert a new entry in the dict, with a new generated id
        
    }

    /*
    Method that handles sending message to the client, via its CH.
    */
    public boolean send(int connectionId, T msg)
    {
        try{
            this.connecetions.get(connectionId).send(msg); //gets the CH via its id and sends the message.
        }
        catch (Exception e){
            return false;
        }
        return true;
    }

    /*
    Method that handles a client desired subscription, sends to the subscription manager.
    */
    public void subscribe(String channel,int connectionId,int subId)
    {
        this.subManager.subscribe(subId,connectionId,channel);
    }

    /*
    Method that handles unsubscribing via the subscruptoin manager.
    */
    public void unsubscribe(int subId, int connectionId)
    {
        this.subManager.unsubscribe(subId,connectionId);
    }

    /*
    Method that sends the message to sub manager to broadcast to the desired channel.
    */
    public void send(String channel, T msg)
    {   
        System.out.println("Sending message from to channel: " + channel +" Msg: " + msg.toString());
        
        String id = ""+idGen.incrementAndGet();
        for(SubscriptionManager.Subscription sub:this.subManager.getSubscriptionsByChannel(channel))
        {
            String message = "MESSAGE\nsubscription:"+sub.getId()+"\nmessage-id:"+id+"\ndestination:"+channel+"\n\n"+msg.toString()+"\0";
            this.send(sub.getConId(),(T)message);
        }
    }
    /*
    Deletes a connection from the dict using the connection ID.
    */
    public void disconnect(int connectionId){
    
        this.connecetions.remove(connectionId);
        this.subManager.disconnectUser(connectionId);
    }

    /*
    Method that checks if a user is subscribed to a channel.
    */
    public boolean isSubscribed(int connectionId, String channel)
    {
        return this.subManager.isSubscribedToChannel(channel,connectionId);
    }
    
}
