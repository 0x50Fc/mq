package org.hailong.mq.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import org.hailong.mq.Message;

/**
 * 客户端
 * @author hailongz
 *
 */
public class TCPClient extends TCP {

	private final Selector _selector;
	private final SocketChannel _tcpChannel;
	
	public TCPClient(SocketAddress address) throws IOException {
		_selector = Selector.open();
		_tcpChannel = SocketChannel.open(address);
		_tcpChannel.configureBlocking(false);
		_tcpChannel.register(_selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
	}
	
	public TCPClient(String hostname,int port) throws IOException {
		this(new InetSocketAddress(hostname, port));
	}
	
	public void select(long timeout) throws IOException {
		
		super.select(timeout);
		
		_selector.select(timeout);
		
		Iterator<SelectionKey> keyIter = _selector.selectedKeys().iterator();
		 
		while (keyIter.hasNext()) {
		 
			SelectionKey key = keyIter.next();
			
			if(key.isConnectable()) {
				emit(new Message("connect","text",key.channel().toString()),this);
			}
			
			if(key.isReadable()) {
				
				Message message;
				
				while((message = read(key)) != null) {
					emit(message, this);
				}
				
			}
			
			if(key.isWritable()) {
				write(key);
			}
			
			keyIter.remove();
			
		}
		
	}

	public void destory() {	
		
		emit(new Message("disconnect","",new byte[0]),this);
		
		try {
			_selector.close();
		}
		catch(Throwable ex){
			ex.printStackTrace();
		}
		
		super.destory();
	}

}
