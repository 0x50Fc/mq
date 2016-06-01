package org.hailong.mq;

import java.nio.ByteBuffer;

/**
 * 消息序列化
 * @author hailongz
 *
 */
public final class MessageIO {

	/**
	 * 头大小
	 */
	public final static int HEAD_SIZE = 16;
	
	/**
	 * 名称长度
	 */
	public final static Bytes name = new Bytes(0,4);
	
	/**
	 * 类型长度
	 */
	public final static Bytes type = new Bytes(4,4);
	
	/**
	 * 内容长度
	 */
	public final static Bytes bytes = new Bytes(8,4);
	
	/**
	 * 签名
	 */
	public final static Bytes sign = new Bytes(12,4);
	
	/**
	 * 消息长度
	 * @return
	 */
	public final static int length(ByteBuffer b) {
		return name.intValue(b) + type.intValue(b) + bytes.intValue(b);
	}
	
	/**
	 * 生产签名
	 * @param name
	 * @param type
	 * @param bytes
	 * @return
	 */
	public final static int genSign(byte[] name,byte[] type,byte[] bytes) {
		
		int v = 0;
		
		for(byte b : name) {
			v += b;
		}
		
		for(byte b : type) {
			v += b;
		}
		
		for(byte b : bytes) {
			v += b;
		}
		
		return v;
	}
	
	public final static Message read(ByteBuffer b) {
		
		if(b.limit() >= MessageIO.HEAD_SIZE) {
			
			int length = MessageIO.length(b);
			
			if(b.limit() >= length + MessageIO.HEAD_SIZE) {
				
				byte[] name = new byte[MessageIO.name.intValue(b)];
				byte[] type = new byte[MessageIO.type.intValue(b)];
				byte[] bytes = new byte[MessageIO.bytes.intValue(b)];
				int sign = MessageIO.sign.intValue(b);
				
				b.position(MessageIO.HEAD_SIZE);
				b.get(name);
				b.get(type);
				b.get(bytes);
				
				b.position(length + MessageIO.HEAD_SIZE);
				b.compact();
				
				if(sign == genSign(name, type, bytes)) {
					return new Message(new String(name,Message.charset),new String(type,Message.charset),bytes);
				}
			}
		}
		
		return null;
	}
	
	public final static ByteBuffer write(Message message, ByteBuffer b, int capacity) {
		
		byte[] name = message.name.getBytes(Message.charset);
		byte[] type = message.type.getBytes(Message.charset);
		byte[] bytes = message.bytes == null ? new byte[0] : message.bytes;
		int length = name.length + type.length + bytes.length;
		
		if(b == null || b.capacity() < length) {
			b = ByteBuffer.allocate(Math.max(length, capacity));
		}
		else {
			b.clear();
		}
		
		MessageIO.name.set(b,name.length);
		MessageIO.type.set(b,type.length);
		MessageIO.bytes.set(b,bytes.length);
		MessageIO.sign.set(b, genSign(name,type,bytes));
		
		b.position(HEAD_SIZE);
		
		b.put(name);
		b.put(type);
		b.put(bytes);
		
		b.flip();
		
		return b;
		
	}
	
	public static class Bytes {
		
		public final int offset;
		public final int size;
		
		public Bytes(int offset,int size) {
			this.offset = offset;
			this.size = size;
		}
		
		public int intValue(ByteBuffer bytes) {
			
			int v = 0;
			
			for(int i=0;i<this.size;i++) {
				v = (v << 8) | bytes.get(this.offset + i);
			}
			
			return v;
		}
		
		public void set(ByteBuffer bytes, int value) {
			for(int i=0;i<this.size;i++) {
				bytes.put(this.offset + this.size - i - 1, (byte) (value & 0x0ff));
				value = value >> 8;
			}
		}
	}
}
