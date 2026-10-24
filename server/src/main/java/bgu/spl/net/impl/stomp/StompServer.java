package bgu.spl.net.impl.stomp;

import java.util.function.Supplier;

import bgu.spl.net.srv.Server;
import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.StompSrv.StompMsgEncDec;
import bgu.spl.net.srv.StompSrv.StompMsgProtocol;

public class StompServer {

    public static void main(String[] args) {
        
        Supplier<StompMessagingProtocol<String>> protocolSupplier =
        () -> new StompMsgProtocol();

        Supplier<MessageEncoderDecoder<String>> encdecSupplier =
        () -> new StompMsgEncDec();

        int port = Integer.parseInt(args[0]);
        if(args[1].equals("tpc"))
        {
            Server.StompThreadPerClient(port, protocolSupplier, encdecSupplier).serve();
        }
        else
        {
            Server.StompReactor(10, port, protocolSupplier, encdecSupplier).serve();
        }
    }
        
}
