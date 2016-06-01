package org.hailong.mq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * 消息队列
 * @author hailongz
 *
 */
public class MQ implements Destory {

	/**
	 * 属性
	 */
	private final Map<String,Object> _attributes = new HashMap<String,Object>(4);
	
	/**
	 * 接收者
	 */
	private final List<_Recipient> _recipients = new LinkedList<_Recipient>();
	
	/**
	 * 运行
	 */
	private final Queue<_Runnable> _runnables = new LinkedBlockingQueue<_Runnable>();
	
	public Object get(String name) {
		return _attributes.containsKey(name) ? _attributes.get(name) : null;
	}
	
	public MQ set(String name,Object value) {
		
		if(value == null) {
			if(_attributes.containsKey(name)) {
				_attributes.remove(name);
			}
		}
		else {
			_attributes.put(name, value);
		}
		
		return this;
	}
	
	public MQ on(Pattern pattern,Recipient recipient) {
	
		_recipients.add(new _Recipient(pattern, recipient));
		
		return this;
	}
	
	public MQ off(Pattern pattern, Recipient recipient) {
		
		for(int i=0;i<_recipients.size();i++) {
			_Recipient r = _recipients.get(i);
			if((pattern == null || pattern.equals(r)) && (recipient == null || recipient == r.recipient)){
				_recipients.remove(i);
				i --;
			}
		}
		
		return this;
	}
	
	protected MQ emit(Message message,MQ from) {
		
		List<_Recipient> rs = new ArrayList<_Recipient>(4);
		
		for(_Recipient r : _recipients) {
			if(r.pattern.matcher(message.name).find()){
				rs.add(r);
			}
		}
		
		for(_Recipient r : rs) {
			r.recipient.onMessage(message,this, from);
		}
		
		return this;
	}
	
	public MQ emit(Message message) {
		return this.emit(message,this);
	}
	
	public MQ post(Runnable runnable, long afterDelay) {
		_Runnable r = new _Runnable(runnable,afterDelay);
		_runnables.add(r);
		return this;
	}
	
	public void select(long timeout) throws IOException {
		
		long b = System.currentTimeMillis();
		
		_Runnable r = _runnables.poll();
		
		while(r != null) {
			
			if(System.currentTimeMillis() - r.ctime >= r.afterDelay) {
				r.runnable.run();
				if(System.currentTimeMillis() - b > timeout) {
					break;
				}
			}
			
			r = _runnables.poll();
		}
		
	}
	
	private static class _Recipient {
		
		public final Pattern pattern;
		public final Recipient recipient;
		
		public _Recipient(Pattern pattern,Recipient recipient) {
			this.pattern = pattern;
			this.recipient = recipient;
		}
	}
	
	private static class _Runnable {
		
		public final long afterDelay;
		public final long ctime;
		public final Runnable runnable;
		
		public _Runnable(Runnable runnable,long afterDelay) {
			this.runnable = runnable;
			this.afterDelay = afterDelay;
			this.ctime = System.currentTimeMillis();
		}
	}

	@Override
	public void destory() {
	
		for(String key : _attributes.keySet()) {
			Object v = _attributes.get(key);
			if(v != null && v instanceof Destory) {
				((Destory)v).destory();
			}
		}
	}
}
