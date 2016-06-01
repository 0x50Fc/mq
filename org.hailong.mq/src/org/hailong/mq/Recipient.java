package org.hailong.mq;

/**
 * 接收者
 * @author hailongz
 *
 */
public interface Recipient {

	public void onMessage(Message message, MQ mq, MQ from) ;
	
}
