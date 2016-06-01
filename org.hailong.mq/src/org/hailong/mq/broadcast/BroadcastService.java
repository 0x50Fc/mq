package org.hailong.mq.broadcast;

import java.util.HashSet;
import java.util.Set;

import org.hailong.mq.MQ;
import org.hailong.mq.Message;
import org.hailong.mq.Recipient;

/**
 * 广播服务
 * @author hailongz
 *
 */
public class BroadcastService implements Recipient {

	private Set<MQ> _froms = new HashSet<MQ>(4);
	
	@Override
	public void onMessage(Message message, MQ mq, MQ from) {
		
		if(message.name.equals("connect")) {
			_froms.add(from);
		}
		else if(message.name.equals("disconnect")) {
			_froms.remove(from);
		}
		else if(message.name.startsWith("broadcast.")) {
			
			for(MQ v : _froms) {
				
				if(v != from) {
					v.emit(message);
				}
			}
		}
	}

}
