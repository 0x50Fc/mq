package org.hailong.mq.message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.hailong.mq.ExecutorService;
import org.hailong.mq.MQ;
import org.hailong.mq.Message;
import org.hailong.mq.Value;

/**
 * 消息服务
 * @author hailongz
 *
 */
public class MessageService extends ExecutorService {

	private final MessageDB _db;
	
	public MessageService(java.util.concurrent.ExecutorService executor,MessageDB db) {
		super(executor);
		_db = db;
	}
	
	public MessageService(MessageDB db) {
		this(Executors.newScheduledThreadPool(4),db);
	}

	private final static Pattern ID = Pattern.compile("[^.]+$");
	
	private Map<Long,Set<MQ>> _users = new HashMap<Long,Set<MQ>>(4);
	
	@Override
	public void onMessage(final Message message, final MQ mq, final MQ from) {
		
		if(message.name.startsWith("broadcast.message.change.")) {
			
			Matcher matcher = ID.matcher(message.name);
			
			if(matcher.find()) {
				
				long uid = Value.longValue(matcher.group(), 0);
				
				if(_users.containsKey(uid)) {
					
					Message change = new Message("message.change","",new byte[0]);
					
					for(MQ v : _users.get(uid)) {
						v.emit(change);
					}
					
				}
				
			}
		}
		else if(message.name.equals("disconnect")) {
			
			List<Long> rm = new ArrayList<Long>(4);
			
			for(Long key : _users.keySet()) {
				Set<MQ> vs = _users.get(key);
				vs.remove(from);
				if(vs.size() == 0) {
					rm.add(key);
				}
			}
			
			for(Long key : rm) {
				_users.remove(key);
			}
			
		}
		else {
			super.onMessage(message, mq, from);
		}
	}
	
	@Override
	protected void onExecutorMessage(Message message, MQ mq, MQ from) {
		
		if(message.name.startsWith("message.session.")) {
			onSession(message,mq,from);
		}
		else if(message.name.startsWith("message.metting.")) {
			onMetting(message,mq,from);
		}
		else if(message.name.startsWith("message.login.")) {
			onLogin(message,mq,from);
		}
		else if(message.name.startsWith("message.pull.")) {
			onPull(message,mq,from);
		}
		
	}
	
	/**
	 * 会话
	 * @param message
	 * @param mq
	 * @param from
	 */
	public void onSession(Message message,final MQ mq,final MQ from) {
		
		Object v = from.get("token");
		
		if(v != null) {
			
			from.post(new Runnable(){

				@Override
				public void run() {
					from.emit(new Message("message.fail","text","未登录"));
				}
				
			}, 0);
			
		}
		else {
			
			Matcher matcher = ID.matcher(message.name);
			
			if(matcher.find()) {
				
				long uid = Value.longValue(matcher.group(), 0);
				
				try {
					
					final long[] uids = _db.add(Value.stringValue(v, null),uid,0,message.type,message.bytes);
					
					mq.post(new Runnable(){

						@Override
						public void run() {
							
							for(long uid : uids) {
								
								mq.emit(new Message("broadcast.message.change." + uid,"",new byte[0]));
							}
							
						}}, 0);
				}
				catch(final Throwable ex) {
					
					from.post(new Runnable(){

						@Override
						public void run() {
							from.emit(new Message("message.fail","text",ex.getMessage()));
						}
						
					}, 0);
					
				}
			}
			
		}
	}
	
	/**
	 * 群聊
	 * @param message
	 * @param mq
	 * @param from
	 */
	public void onMetting(Message message,final MQ mq,final MQ from) {
		
		Object v = from.get("token");
		
		if(v != null) {
			
			from.post(new Runnable(){

				@Override
				public void run() {
					from.emit(new Message("message.fail","text","未登录"));
				}
				
			}, 0);
			
		}
		else {
			
			Matcher matcher = ID.matcher(message.name);
			
			if(matcher.find()) {
				
				long mettingId = Value.longValue(matcher.group(), 0);
				
				try {
					
					final long[] uids = _db.add(Value.stringValue(v, null),0,mettingId,message.type,message.bytes);
					
					mq.post(new Runnable(){

						@Override
						public void run() {
							
							for(long uid : uids) {
								
								mq.emit(new Message("broadcast.message.change." + uid,"",new byte[0]));
							}
							
						}}, 0);
					
				}
				catch(final Throwable ex) {
					
					from.post(new Runnable(){

						@Override
						public void run() {
							from.emit(new Message("message.fail","text",ex.getMessage()));
						}
						
					}, 0);
					
				}
			}
			
		}
	}
	
	/**
	 * 登录
	 * @param message
	 * @param mq
	 * @param from
	 */
	public void onLogin(Message message,MQ mq,final MQ from) {
		
		final String token = new String(message.bytes,Message.charset);
		
		try {
			
			final long uid = _db.auth(token);
			
			from.post(new Runnable(){

				@Override
				public void run() {
					
					from.set("uid", uid);
					from.set("token", token);
					
					Set<MQ> mqs;
					
					if(_users.containsKey(uid)) {
						mqs = _users.get(uid);
					}
					else {
						mqs = new HashSet<MQ>(2);
						_users.put(uid, mqs);
					}
					
					mqs.add(from);
					
					from.emit(new Message("message.login","text",String.valueOf(uid)));
				}
				
			}, 0);
			
		}
		catch(final Throwable ex) {
			
			from.post(new Runnable(){

				@Override
				public void run() {
					from.emit(new Message("message.fail","text",ex.getMessage()));
				}
				
			}, 0);
			
		}
		
	}
	
	/**
	 * 拉取
	 * @param message
	 * @param mq
	 * @param from
	 */
	public void onPull(Message message,MQ mq,final MQ from) {
		
		Object v = from.get("token");
		
		if(v != null) {
			
			from.post(new Runnable(){

				@Override
				public void run() {
					from.emit(new Message("message.fail","text","未登录"));
				}
				
			}, 0);
			
		}
		else {

			try {
				
				long objectId = Value.longValue(new String(message.bytes,Message.charset), 0);
				
				final Message[] messages = _db.pull(Value.stringValue(v, null),objectId,200);
				
				from.post(new Runnable(){

					@Override
					public void run() {
						for(Message message : messages) {
							from.emit(message);
						}
					}
					
				}, 0);
				
				
			}
			catch(final Throwable ex) {
				
				from.post(new Runnable(){

					@Override
					public void run() {
						from.emit(new Message("message.fail","text",ex.getMessage()));
					}
					
				}, 0);
				
			}
			
		}
	}

	
	
}
