package org.hailong.mq.udp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.hailong.mq.MQ;
import org.hailong.mq.Message;
import org.hailong.mq.MessageIO;

/**
 * UDP 消息队列
 * @author hailongz
 *
 */
public class UDP extends MQ {

	/**
	 * 最大包大小
	 */
	public static final int MAX_SIZE = 20480;
	
	/**
	 * 超时时间
	 */
	public static final long keepAlive = 6000;
	
	private final Selector _selector;
	private final DatagramChannel _channel;
	
	private Map<SocketAddress,Client> _clients = new HashMap<SocketAddress,Client>(4);
	private Queue<_Message> _messages = new LinkedList<_Message>();
	private ByteBuffer _rd;
	private ByteBuffer _wd;
	
	public UDP(SocketAddress address) throws IOException {
		_selector = Selector.open();
		_channel = DatagramChannel.open();
		_channel.configureBlocking(false);
		_channel.register(_selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
		_channel.bind(address);
	}

	public UDP(int port) throws IOException {
		this(new InetSocketAddress(port));
	}
	
	public MQ emit(Message message,SocketAddress address) {
		
		Client client;
		
		if(_clients.containsKey(address)) {
			client = _clients.get(address); 
			client.atime = System.currentTimeMillis();
		}
		else {
			client = new Client(address);
			_clients.put(address, client);
			emit(new Message("connect","",new byte[0]),client);
		}
		
		_messages.add(new _Message(message,client));
		
		return this;
	}
	
	private _Message read(SelectionKey key) throws IOException {
		
		DatagramChannel ch = (DatagramChannel) key.channel();
	
		if(_rd == null) {
			_rd = ByteBuffer.allocate(MAX_SIZE);
		}
		else {
			_rd.clear();
		}
		
		SocketAddress address;
		
		while(true) {
			
			if((address = ch.receive(_rd)) != null) {
				
				if(_rd.limit() >= MessageIO.HEAD_SIZE) {
					
					int length = MessageIO.length(_rd);
					
					if(_rd.limit() >= length + MessageIO.HEAD_SIZE) {
						
						Message message = MessageIO.read(_rd);
						
						if(message != null) {
							
							Client client;
							
							if(_clients.containsKey(address)) {
								client = _clients.get(address); 
								client.atime = System.currentTimeMillis();
							}
							else {
								client = new Client(address);
								_clients.put(address, client);
								emit(new Message("connect","",new byte[0]),client);
							}
							
							return new _Message(message,client);
							
						}
					}
					else {
						_rd.clear();
					}
				}
				else {
					_rd.clear();
				}
			}
			else {
				break;
			}
		}
		

		return null;
	}

	private void write(SelectionKey key) throws IOException {
		
		DatagramChannel ch = (DatagramChannel) key.channel();
		
		if(_wd == null) {
			_wd = ByteBuffer.allocate(MAX_SIZE);
		}
		else {
			_wd.clear();
		}
		
		_Message message = _messages.poll();
		
		if(message != null) {
			_wd = MessageIO.write(message.message, _wd, MAX_SIZE);
			ch.send(_wd, message.client.address);
		}
		
	}
	
	public void select(long timeout) throws IOException {
		
		super.select(timeout);
		
		_selector.select(timeout);
		
		Iterator<SelectionKey> keyIter = _selector.selectedKeys().iterator();
		 
		while (keyIter.hasNext()) {
		 
			SelectionKey key = keyIter.next();
			
			if(key.isReadable()) {
				
				_Message message;
				
				while((message = read(key)) != null) {
					emit(message.message, message.client);
				}
				
			}
			
			if(key.isWritable()) {
				write(key);
			}
			 
			keyIter.remove();
		 
		}
		
		List<SocketAddress> rm = new LinkedList<SocketAddress>();
		
		for(SocketAddress key : _clients.keySet()) {
			Client v = _clients.get(key);
			if(System.currentTimeMillis() - v.atime > keepAlive) {
				emit(new Message("disconnect","",new byte[0]),v);
				rm.add(key);
			}
		}
		
		for(SocketAddress r: rm) {
			_clients.remove(r);
		}
	}
	
	public void destory() {
		
		for(SocketAddress key : _clients.keySet()) {
			Client v = _clients.get(key);
			emit(new Message("disconnect","",new byte[0]),v);
		}
		
		try {
			_selector.close();
		}
		catch(Throwable ex) {
			ex.printStackTrace();
		}
		
		super.destory();
	}
	
	private static class _Message {
		
		public final Message message;
		public final Client client;
		
		public _Message(Message message,Client client) {
			this.message = message;
			this.client = client;
		}
		
	}
	
	private class Client extends MQ {
		
		public final SocketAddress address;
		public long atime;
		
		public Client(SocketAddress address) {
			this.address = address;
			this.atime = System.currentTimeMillis();
		}
		
		@Override
		public MQ emit(Message message) {
			UDP.this.emit(message, address);
			return this;
		}
		
	}
	
}
