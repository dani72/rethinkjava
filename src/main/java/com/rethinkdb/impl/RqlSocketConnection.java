package com.rethinkdb.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;

/**
 *
 * @author dani
 */
public class RqlSocketConnection {
    
    private final SocketChannel _sc;
    
    public RqlSocketConnection( String hostname, int port) throws IOException {
        _sc = SocketChannel.open();
        _sc.configureBlocking(true);
        _sc.connect( new InetSocketAddress( hostname, port));

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(com.rethinkdb.Ql2.VersionDummy.Version.V0_1_VALUE);

        buffer.flip();

        while (buffer.hasRemaining()) {
            _sc.write(buffer);
        }
    }
    
    public void close() throws IOException {
        _sc.close();
    }
    
    public void send( byte[] data) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        while( buffer.hasRemaining()) {
            _sc.write(buffer);
        }
    }

    public ByteBuffer receive() throws IOException {
        ByteBuffer datalen = ByteBuffer.allocate(4);
        datalen.order(ByteOrder.LITTLE_ENDIAN);

        int bytesRead = _sc.read(datalen);
        
        if( bytesRead != 4) {
            throw new IOException( "Incorrect amount of bytes read: " + bytesRead + " (expected 4).");
        }
        
        datalen.flip();
        
        int len = datalen.getInt();
        ByteBuffer buf = ByteBuffer.allocate(len);
        bytesRead = 0;

        while (bytesRead != len) {
            bytesRead += _sc.read(buf);
        }
        
        buf.flip();
        
        return buf;
    }
}
