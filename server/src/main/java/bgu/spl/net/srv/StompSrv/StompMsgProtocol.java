package bgu.spl.net.srv.StompSrv;

import java.util.HashMap;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.data.*;
import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;

public class StompMsgProtocol implements StompMessagingProtocol<String> {

    private Connections<String> cManager;
    private int conId;
    private String userName = "";
    private boolean shouldTerminate = false;

    private Database db = Database.getInstance();
    

    /*
    Method that inits the protocl
    */
    public void start(int connectionId, Connections<String> connections)
    {
        this.cManager = connections;
        this.conId = connectionId;
    }

    /*
    Proccessing a recived frame and acting accordingly.
    */
    public void process(String message)
    {
        HashMap<String, String> headers = new HashMap<>();
        try{
            String[] lines = message.split("\\R"); // Split by new line characters
            if (lines.length == 0 || lines[0].isEmpty()) {
                throw new IllegalArgumentException("Empty message or missing command");
            }
            String command = lines[0];
            
            int i = 1;

            while (i < lines.length && !lines[i].isEmpty() && lines[i].charAt(0) != '\0') { // constructing headers
                String[] temp = lines[i].split(":", 2); // Split only on the first colon
                if (temp.length < 2) {
                    throw new IllegalArgumentException("Header line is incorrect format.");
                }
                headers.put(temp[0], temp[1]);
                i++;
            }

           String body = "";

            for (int j = i + 1; j < lines.length; j++) {
                body+=lines[j];
                if (j < lines.length - 1) {
                    body+="\n";
                }
            } // constructing body
        
            switch (command) { // acting by the command
                case "CONNECT": 
                    solveConnect(headers);
                    if(headers.containsKey("receipt"))
                        sendRecipt(Integer.parseInt(headers.get("receipt")));
                    break;
                case "SEND":
                    conCheck();
                    solveSend(headers, body);
                    if(headers.containsKey("receipt"))
                        sendRecipt(Integer.parseInt(headers.get("receipt")));
                    break;
                case "SUBSCRIBE":
                    conCheck();
                    solveSubscribe(headers);
                    if(headers.containsKey("receipt"))
                        sendRecipt(Integer.parseInt(headers.get("receipt")));
                    break;
                case "UNSUBSCRIBE":
                    conCheck();
                    solveUnsubscribe(headers);
                    if(headers.containsKey("receipt"))
                        sendRecipt(Integer.parseInt(headers.get("receipt")));
                    break;
                case "DISCONNECT":
                    conCheck();
                    if(headers.containsKey("receipt"))
                        sendRecipt(Integer.parseInt(headers.get("receipt")));

                    handleDisconnect();
                    break;
                default://ERROR
                    throw new RuntimeException("Unknown command " + command);
            }
            
        }
        catch (Exception e){ // if any errors were thrown in the process, send an error frame and close
            System.out.println(e.getMessage() + " On Connection ID: " + conId);
            solveError(e.getMessage(),headers.get("receipt"));
        }

    }

    /*
    A method that handles connect command.
    */
    private void solveConnect(HashMap<String,String> headers)
    {
        if(!headers.containsKey("login") || !headers.containsKey("passcode"))
            throw new IllegalArgumentException("Did not contain login or passcode headers, which are required for connection.");

        String user = headers.get("login");
        String pass = headers.get("passcode");

        LoginStatus status = this.db.login(conId, user, pass); // trying to login,as well as log in the db server

        if(status == LoginStatus.ALREADY_LOGGED_IN)
            throw new RuntimeException("User already logged in.");
        else if(status == LoginStatus.CLIENT_ALREADY_CONNECTED)
            throw new RuntimeException("Client already connected.");
        else if(status == LoginStatus.WRONG_PASSWORD)
            throw new RuntimeException("Wrong password for user:" + user);

        this.userName = user; //here either the user was added or successfuly connected
        System.out.println("User:" + userName+" connected on id:" + conId);
        cManager.send(conId, "CONNECTED\nversion:1.2\n\n\0");
    }
    
    /*
    Method that handles send command
    */
    private void solveSend(HashMap<String,String> headers,String body)
    {
       if(!headers.containsKey("destination"))
           throw new IllegalArgumentException("Did not contain a destination header, which is required for message propagation.");

       if(!cManager.isSubscribed(conId,headers.get("destination")))
           throw new IllegalArgumentException("Cant send message to a channel the user is not subscribed to.");

       String channel = headers.get("destination");
       if(headers.containsKey("file-name")) // if it was the first frame of the report it should have a file-name header.
            this.db.trackFileUpload(this.userName,headers.get("file-name"),channel); // log in db server
       
       cManager.send(channel,body);
    }

    /*
    Method that solves subscribes command, using the helper subscription manager class.
    */
    private void solveSubscribe(HashMap<String,String> headers)
    {
        if(!headers.containsKey("destination"))
           throw new IllegalArgumentException("Did not contain a destination header, which is required for subscription.");
        else if(!headers.containsKey("id"))
           throw new IllegalArgumentException("Did not contain an id header, which is required for subscription.");

        String channel = headers.get("destination");
        int subId = Integer.parseInt(headers.get("id"));
        cManager.subscribe(channel,conId,subId);

        System.out.println("User with id: " + conId + " subscribed to channel: " + channel + " with subscription id: " + subId);
    }

    /*
    Method that solves unsubscribe command, using the sub manger that is inside the connection manager.
    */
    private void solveUnsubscribe(HashMap<String,String> headers)
    {
        if(!headers.containsKey("id"))
           throw new IllegalArgumentException("Did not contain an id header, which is required for unsubscription.");

        int subId = Integer.parseInt(headers.get("id"));
        cManager.unsubscribe(subId,conId);
        System.out.println("User with id: " + conId + " unsubscribed from subscription id: " + subId);
    }

    /*
    Method that responsible sending an error frame to the client and close, in case of an error.
    */
    private void solveError(String errorMsg,String receipt)
    {
        String res="";
        if(receipt != null)
            res = "ERROR\nreceipt-id: message-" + receipt + "\nmessage:"+ errorMsg+"\n\n\0";
        else
            res = "ERROR\nmessage:"+ errorMsg+"\n\n\0";
        cManager.send(conId, res);
        handleDisconnect();
    }

    /*
    Method that handles client disconnection from the server. either by logout command or by an error.
    */
    public void handleDisconnect()
    {
        System.out.println("Disconnecting user:" + userName + " from connection: " + conId);
        this.shouldTerminate = true;
        cManager.disconnect(conId);
        if(db.isLoggedIn(userName))
            this.db.logout(conId);
        userName = "";
    }
	/**
     * @return true if the connection should be terminated
     */
    public boolean shouldTerminate()
    {
        return shouldTerminate;
    }
    
    /*
    Method that checks if the handler is connected to the server.
    if not, throws an exception.
    */
    public void conCheck()
    {
        if(userName == "" || !db.isLoggedIn(userName))
            throw new IllegalStateException("Client is not logged in to the server.");
    }

    /*
    Handles receipt frame sending to the client.
    */
    public void sendRecipt(int reciptId)
    {
        String res = "RECEIPT\nreceipt-id:"+reciptId+"\n\n\0";
        cManager.send(conId, res);
    }

}
