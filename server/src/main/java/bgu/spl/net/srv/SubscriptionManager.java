package bgu.spl.net.srv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class SubscriptionManager
{
    /*
    The subscriptionManager class, Handles subscrition maintance and allowes message sending to a subscriptions.
    
    */
    public class Subscription {

        /*
        Subclass that represents an active subscription.
        Saves subscription id
        ConnectionHandler object to enable client communication.
        Name of the subscription channel.
        */
        private int subId;
        private int conId;
        private String channel;
        
        /*
        Constructs a subscription
        */
        public Subscription(int subId,int conId,String channel)
        {
            this.channel = channel;
            this.subId =subId;
            this.conId = conId;
        }

        public int getId(){return this.subId;}

        public int getConId(){return this.conId;}

        public String getChannel(){return this.channel;}

    };
    private ConcurrentHashMap<String,List<Subscription>> subscriptions; // Saves the subscriptions by their channel name.
    private ConcurrentHashMap<Integer,List<Subscription>> subDict; // Saves the subscriptions by ConnectionID.


    /*
    Construct the sub-manager.
    */
    public SubscriptionManager()
    {
        this.subscriptions = new ConcurrentHashMap<>();
        this.subDict = new ConcurrentHashMap<>();
        
    }

    /*
    Method that handles creating a new list of subscriptions if a new topic was inserted.
    */
    public void addTopicIfNeeded(String channel){
        if(!this.subscriptions.containsKey(channel))
            this.subscriptions.put(channel, new CopyOnWriteArrayList<>());
    }

    /*
    Removes a topic from the dict when there are no subscriptoins to it.
    */
    public void removeTopicIfNeeded(String channel){
        if(this.subscriptions.containsKey(channel) && this.subscriptions.get(channel).size()==0)
            this.subscriptions.remove(channel);
    }


    /*
    Handles user subscription to a channel.
    */
    public void subscribe(int subId,int conId,String channel){

        List<Subscription> list = this.subDict.get(conId);
        if(list!= null){
            for(Subscription s:list)
            {
                if(s.getChannel().equals(channel))
                    throw new RuntimeException("Connection already subscribed to the topic");
            }
        }

        Subscription toAdd = new Subscription(subId,conId,channel);
        addTopicIfNeeded(channel); // adds topic

        this.subscriptions.get(channel).add(toAdd);
        if(!this.subDict.containsKey(conId)) //register in the dict if the user is new.
        {
            this.subDict.put(conId,new CopyOnWriteArrayList<>());
        }
        this.subDict.get(conId).add(toAdd); //adds to the user dict.
    }

    /*
    method that returns the list of subscriptions subscribed to a channel.
    */
    public List<Subscription> getSubscriptionsByChannel(String channel)
    {
        return this.subscriptions.get(channel);
    }

    /*
    Method that handles unsubscribing from a channel, using the subscription ID.
    */
    public void unsubscribe(int subscriptionId,int connectionId)
    {
        if(!subDict.containsKey(connectionId))
            throw new RuntimeException("Cannot cancel subscription on a non existing user.");
        
        List<Subscription> list = this.subDict.get(connectionId);
        for(int i=0;i<list.size();i=i+1)
        {
            Subscription s = list.get(i);
            if(s.getId() == subscriptionId)
            {
                removeSub(s);
                return;
            }
        }
    }
    /*
    Method that handles unsubscribing from all the channels that the user is subscribed to.
    */
    public void disconnectUser(int connectionId)
    {
        if(!this.subDict.containsKey(connectionId))
            return;

        List<Subscription> list = this.subDict.get(connectionId);
        for(Subscription s:list) // going over user's subscription
        {
            removeSub(s);
        }
        this.subDict.remove(connectionId); // remove user entry
    }

    /*
    Helper method that removes a subscription from the channel mapping.
    */
    private void removeSub(Subscription s)
    {
        System.out.println("Removing subscription id: " + s.getId() + " from channel: " + s.getChannel() + " for user id: " + s.getConId());
        this.subscriptions.get(s.getChannel()).remove(s); //removes it from the list (by channel)
        this.subDict.get(s.getConId()).remove(s); // removes it from the user list (by connectionId)
        this.removeTopicIfNeeded(s.getChannel());

    }

    /*
    Method that checks if a user is subscribed to a given channel.
    returns true iff user is subscribed to the input channel.
    */
    public boolean isSubscribedToChannel(String channel,int connectionId)
    {
        if(!this.subscriptions.containsKey(channel))
             return false;
        for(Subscription s:this.subscriptions.get(channel))
        {
            if(s.getConId() == connectionId)
                return true;
        }

        return false;
    }

}
