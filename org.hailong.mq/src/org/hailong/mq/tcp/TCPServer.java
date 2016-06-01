package org.hailong.mq.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import org.hailong.mq.MQ;
import org.hailong.mq.Message;

/**
 * 服务端
 * @author hailongz
 *
 */
public class TCPServer extends MQ {

	private final Selector _selector;
	private final ServerSocketChannel _tcpChannel;
	
 	public TCPServer(SocketAddress address) throws IOException {
		
		_selector = Selector.open();
		
		_tcpChannel = ServerSocketChannel.open();
		_tcpChannel.configureBlocking(false);
		_tcpChannel.register(_selector, SelectionKey.OP_ACCEPT);
		_tcpChannel.bind(address);

	}

	public TCPServer(int port) throws IOException {
		this(new InetSocketAddress(port));
	}
	
	protected void onAccept(SelectionKey key) throws IOException {
		
		ServerSocketChannel channel = (ServerSocketChannel) key.channel();
		
		SocketChannel ch = channel.accept();
		
		TCP client = new TCP();
		
		ch.configureBlocking(false);
		ch.register(_selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE,client);
	
		emit(new Message("connect","text",ch.toString()),client);
		
	}
	
	public void select(long timeout) throws IOException {
		
		super.select(timeout);
		
		_selector.select(timeout);
		
		Iterator<SelectionKey> keyIter = _selector.selectedKeys().iterator();
		 
		while (keyIter.hasNext()) {
		 
			SelectionKey key = keyIter.next();
			 
			if(key.channel() == _tcpChannel) {
				onAccept(key);
			}
			else {
				
				TCP client = (TCP) key.attachment();
				
				try {
					
					client.select(timeout);
					
					if(key.isReadable()) {
						
						Message message;
						
						while((message = client.read(key)) != null) {
							emit(message, client);
						}
						
					}
					
					if(key.isWritable()) {
						client.write(key);
					}
					
				}
				catch(IOException ex) {
					emit(new Message("disconnect","text",key.channel().toString()),client);
					client.destory();
					key.cancel();
				}
			}
			 
			keyIter.remove();
		 
		}
		
	}
	
	public void destory() {	
		
		try {
			
			for(SelectionKey key : _selector.keys()) {
				if(key.isValid() && key.channel() != _tcpChannel) {
					TCP client = (TCP) key.attachment();
					emit(new Message("disconnect","",new byte[0]),client);
					client.destory();
				}
			}
			
			_selector.close();
		}
		catch(Throwable ex){
			ex.printStackTrace();
		}
		
		super.destory();
	}
	
}
