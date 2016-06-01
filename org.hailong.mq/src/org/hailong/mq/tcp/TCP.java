package org.hailong.mq.tcp;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;
import org.hailong.mq.MQ;
import org.hailong.mq.Message;
import org.hailong.mq.MessageIO;

public class TCP extends MQ {

	public static final int RD_CAPACITY = 2048;
	public static final int WD_CAPACITY = 2048;

	private Queue<Message> _messages = new LinkedList<Message>();
	private ByteBuffer _rd;
	private ByteBuffer _wd;
	private boolean _destoryed;
	
	@Override
	public MQ emit(Message message) {
		
		if(! _destoryed) {
			_messages.add(message);
		}
		
		return this;
	}
	
	public Message read(SelectionKey key) throws IOException {
		
		SocketChannel ch = (SocketChannel) key.channel();
		
		if(_rd == null) {
			_rd = ByteBuffer.allocate(RD_CAPACITY);
		}
		
		while(true) {
			
			int r = ch.read(_rd);
			
			if(r > 0) {
				
				if(_rd.limit() >= MessageIO.HEAD_SIZE) {
					
					int length = MessageIO.length(_rd);
					
					if(_rd.limit() >= length + MessageIO.HEAD_SIZE) {
						return MessageIO.read(_rd);
					}
					else if(length + MessageIO.HEAD_SIZE > _rd.capacity()) {
						ByteBuffer b = ByteBuffer.allocate(length + MessageIO.HEAD_SIZE);
						_rd.flip();
						_rd.compareTo(b);
						_rd = b;
						_rd.position(_rd.limit());
					}
				}
				
			}
			else if(r == -1) {
				throw new EOFException();
			}
			else {
				break;
			}
		}
		

		return null;
	}

	public void write(SelectionKey key) throws IOException {
		
		SocketChannel ch = (SocketChannel) key.channel();
		
		if(_wd != null && _wd.hasRemaining()) {
			ch.write(_wd);
			return;
		}
		
		if(_wd == null || ! _wd.hasRemaining() ) {
			
			Message message = _messages.poll();
			
			if(message != null) {
				_wd = MessageIO.write(message, _wd, WD_CAPACITY);
				if(-1 == ch.write(_wd)) {
					throw new EOFException();
				}
			}
		}
	}
	
	public void destory() {
		_destoryed = true;
		super.destory();
	}
	
}
