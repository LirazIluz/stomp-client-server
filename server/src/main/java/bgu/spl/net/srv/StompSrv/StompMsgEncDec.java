package bgu.spl.net.srv.StompSrv;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import bgu.spl.net.api.MessageEncoderDecoder;

public class StompMsgEncDec implements MessageEncoderDecoder<String> {

    ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

    /*
    Decodes bytes read from socket, construct a frame when a delimiter is read ('\0')
    */
    public String decodeNextByte(byte nextByte)
    {
        if(nextByte == 0)
        { 
            byteBuffer.flip();                      
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            byteBuffer.clear();                     
            return new String(bytes, StandardCharsets.UTF_8);
        }
        byteBuffer.put(nextByte);
        return null;
    }

    /*
    Method that encodes a message before transport.
    */
    public byte[] encode(String message)
    {
        return message.getBytes(StandardCharsets.UTF_8);
    }
    
}
