package org.hailong.mq.broadcast;

import org.hailong.mq.MQ;
import org.hailong.mq.Message;
import org.hailong.mq.Recipient;

/**
 * 路由
 * @author hailongz
 *
 */
public class RemoteService implements Recipient {

	private final MQ _mq;
	
	public RemoteService(MQ mq) {
		_mq = mq;
	}
	
	@Override
	public void onMessage(Message message, MQ mq, MQ from) {
		_mq.emit(message);
	}

}
