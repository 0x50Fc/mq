package org.hailong.mq;

import java.nio.charset.Charset;

/**
 * 消息
 * @author hailongz
 *
 */
public class Message {


	/**
	 * 字符集
	 */
	public final static Charset charset = Charset.forName("utf-8");
	
	/**
	 * 消息名
	 */
	public final String name;
	
	/**
	 * 类型
	 */
	public final String type;
	
	/**
	 * 内容
	 */
	public final byte[] bytes;

	public Message(String name,String type,byte[] bytes) {
		this.name = name;
		this.type = type;
		this.bytes = bytes;
	}
	
	public Message(String name,String type,String text) {
		this(name,type,text.getBytes(charset));
	}
	
	/**
	 * 会话消息
	 * @param sessionId
	 * @return
	 */
	public static Message session(String sessionId) {
		return new Message("session","text",sessionId.getBytes(charset));
	}
}
