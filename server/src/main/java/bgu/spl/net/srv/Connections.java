package bgu.spl.net.srv;

import java.io.IOException;

public interface Connections<T> {

    void connect(ConnectionHandler<T> newClient,int conId);

    boolean send(int connectionId, T msg);

    void subscribe(String channel,int connectionId,int subId);

    void unsubscribe(int subId, int connectionId);

    boolean isSubscribed(int connectionId, String channel);

    void send(String channel, T msg);

    void disconnect(int connectionId);
}
